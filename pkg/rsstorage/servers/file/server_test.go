package file

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal/servertest"
	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type FakeReadCloser struct {
	io.Reader
}

func (*FakeReadCloser) Close() error {
	return nil
}

type FileStorageServerSuite struct {
	tempDirHelper servertest.TempDirHelper
}

func (s *FileStorageServerSuite) SetUpTest(c *check.C) {
	s.tempDirHelper = servertest.TempDirHelper{}
	c.Assert(s.tempDirHelper.SetUp(), check.IsNil)
}

func (s *FileStorageServerSuite) TearDownTest(c *check.C) {
	c.Assert(s.tempDirHelper.TearDown(), check.IsNil)
}

var _ = check.Suite(&FileStorageServerSuite{})

func (s *FileStorageServerSuite) TestNew(c *check.C) {
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	debugLogger := &servertest.TestLogger{}
	server := NewStorageServer(StorageServerArgs{
		Dir:          "test",
		ChunkSize:    4096,
		Waiter:       wn,
		Notifier:     wn,
		Class:        "classname",
		DebugLogger:  debugLogger,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute * 2,
	})

	c.Check(server, check.DeepEquals, &StorageServer{
		dir:    "test",
		fileIO: &defaultFileIO{},
		chunker: &internal.DefaultChunkUtils{
			ChunkSize: 4096,
			Server: &StorageServer{
				dir:          "test",
				fileIO:       &defaultFileIO{},
				cacheTimeout: time.Minute,
				walkTimeout:  time.Minute * 2,
				class:        "classname",
				debugLogger:  debugLogger,
			},
			Waiter:      wn,
			Notifier:    wn,
			PollTimeout: rsstorage.DefaultChunkPollTimeout,
			MaxAttempts: rsstorage.DefaultMaxChunkAttempts,
		},
		cacheTimeout: time.Minute,
		walkTimeout:  time.Minute * 2,
		class:        "classname",
		debugLogger:  debugLogger,
	})

	c.Assert(server.Dir(), check.Equals, "test")
	c.Assert(server.Type(), check.Equals, rsstorage.StorageTypeFile)
}

type fakeFileIO struct {
	open           fileIOFile
	openErr        error
	openStaging    fileIOFile
	openStagingErr error
	renamed        int
	stagingToPerm  error
	permanent      string
	removed        int
	remove         error
	removedAll     int
	removeAll      error
	flush          error
	mkdir          error
	stat           os.FileInfo
	statErr        error
}

func (f *fakeFileIO) Stat(name string) (os.FileInfo, error) {
	return f.stat, f.statErr
}

func (f *fakeFileIO) MkdirAll(name string, perm os.FileMode) error {
	return f.mkdir
}

func (f *fakeFileIO) Open(name string) (fileIOFile, error) {
	return f.open, f.openErr
}

func (f *fakeFileIO) OpenStaging(dir, prefix string) (fileIOFile, error) {
	return f.openStaging, f.openStagingErr
}

func (f *fakeFileIO) Move(stagedAt, permanent string) error {
	f.renamed++
	if f.stagingToPerm == nil {
		f.permanent = permanent
	}
	return f.stagingToPerm
}

func (f *fakeFileIO) Remove(location string) error {
	f.removed++
	return f.remove
}

func (f *fakeFileIO) RemoveAll(location string) error {
	f.removedAll++
	return f.removeAll
}

func (f *fakeFileIO) FlushWithChownAndStat(location string) error {
	return f.flush
}

type fakeFileStat struct {
	ts  time.Time
	sz  int64
	dir bool
}

func (f *fakeFileStat) Name() string {
	return "name"
}

func (f *fakeFileStat) Size() int64 {
	return f.sz
}

func (f *fakeFileStat) Mode() os.FileMode {
	return os.FileMode(0)
}

func (f *fakeFileStat) ModTime() time.Time {
	return f.ts
}

func (f *fakeFileStat) IsDir() bool {
	return f.dir
}

func (f *fakeFileStat) Sys() interface{} {
	return nil
}

type FakeFileIOFile struct {
	name     string
	close    error
	contents *bytes.Buffer
	stat     os.FileInfo
	statErr  error
}

func (f *FakeFileIOFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *FakeFileIOFile) Stat() (os.FileInfo, error) {
	return f.stat, f.statErr
}

func (f *FakeFileIOFile) Name() string {
	return f.name
}

func (f *FakeFileIOFile) Close() error {
	return f.close
}

func (f *FakeFileIOFile) Read(p []byte) (n int, err error) {
	return f.contents.Read(p)
}

func (f *FakeFileIOFile) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (s *FileStorageServerSuite) TestCheckOpenErr(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			statErr: errors.New("open error"),
		},
	}
	ok, _, _, _, err := server.Check("", "storageaddress")
	c.Check(ok, check.Equals, false)
	c.Check(err, check.ErrorMatches, "open error")
}

func (s *FileStorageServerSuite) TestCheckNotExist(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			statErr: os.ErrNotExist,
		},
	}
	ok, _, _, _, err := server.Check("", "storageaddress")
	c.Check(ok, check.Equals, false)
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestCheckOk(c *check.C) {
	now := time.Now()
	stat := &fakeFileStat{
		ts: now,
		sz: 65,
	}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			stat: stat,
		},
	}
	ok, chunked, sz, ts, err := server.Check("", "storageaddress")
	c.Check(ok, check.Equals, true)
	c.Assert(chunked, check.IsNil)
	c.Check(sz, check.Equals, int64(65))
	c.Check(ts, servertest.TimeEquals, now)
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestCheckChunked(c *check.C) {
	now := time.Now()
	nowbytes, err := now.MarshalJSON()
	c.Assert(err, check.IsNil)
	info := []byte(`{malformed:"}`)
	buf := bytes.NewBuffer(info)
	stat := &fakeFileStat{
		ts:  now,
		sz:  65,
		dir: true,
	}
	file := &FakeFileIOFile{
		name:     "info.json",
		contents: buf,
	}
	fio := &fakeFileIO{
		stat:    stat,
		open:    file,
		openErr: errors.New("info open error"),
	}
	server := &StorageServer{
		fileIO: fio,
	}

	ok, _, _, _, err := server.Check("", "storageaddress")
	c.Assert(ok, check.Equals, false)
	c.Assert(err, check.ErrorMatches, "no chunked directory 'info.json' for storageaddress: info open error")

	fio.openErr = nil
	ok, _, _, _, err = server.Check("", "storageaddress")
	c.Assert(ok, check.Equals, false)
	c.Assert(err, check.ErrorMatches, "error decoding chunked directory 'info.json' for storageaddress: .+")

	info = []byte(fmt.Sprintf(`{"chunk_size":64,"file_size":3232,"num_chunks":15,"complete":true,"mod_time":%s}`, string(nowbytes)))
	buf = bytes.NewBuffer(info)
	file.contents = buf
	ok, chunked, sz, ts, err := server.Check("", "storageaddress")
	c.Check(ok, check.Equals, true)
	c.Check(chunked, check.NotNil)
	c.Check(sz, check.Equals, int64(3232))
	c.Check(ts, servertest.TimeEquals, now)
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestGetOpenErr(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openErr: errors.New("open error"),
		},
	}
	r, _, _, _, ok, err := server.Get("", "storageaddress")
	c.Check(r, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(err, check.ErrorMatches, "open error")
}

func (s *FileStorageServerSuite) TestGetNotExist(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openErr: os.ErrNotExist,
		},
	}
	r, _, _, _, ok, err := server.Get("", "storageaddress")
	c.Check(r, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestGetOk(c *check.C) {
	f := &FakeFileIOFile{
		stat: &fakeFileStat{},
	}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			open: f,
		},
	}
	r, ch, _, _, ok, err := server.Get("", "storageaddress")
	c.Check(r, check.DeepEquals, f)
	c.Check(ok, check.Equals, true)
	c.Assert(ch, check.IsNil)
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestGetChunked(c *check.C) {
	f := &FakeFileIOFile{
		stat: &fakeFileStat{
			dir: true,
		},
	}
	b := bytes.NewBufferString("hello, I am some data")
	rc := &FakeReadCloser{
		Reader: b,
	}
	now := time.Now()
	chunker := &servertest.DummyChunkUtils{
		Read: rc,
		ReadCh: &types.ChunksInfo{
			Complete: true,
		},
		ReadSz:  int64(b.Len()),
		ReadMod: now,
		ReadErr: errors.New("chunked read error"),
	}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			open: f,
		},
		chunker: chunker,
	}
	r, _, _, _, ok, err := server.Get("", "storageaddress")
	c.Check(err, check.ErrorMatches, "error reading chunked directory files for storageaddress: chunked read error")
	c.Check(ok, check.Equals, false)
	c.Check(r, check.IsNil)

	chunker.ReadErr = nil
	r, ch, sz, mod, ok, err := server.Get("", "storageaddress")
	c.Check(err, check.IsNil)
	c.Check(r, check.DeepEquals, rc)
	c.Check(ok, check.Equals, true)
	c.Check(ch, check.DeepEquals, &types.ChunksInfo{
		ChunkSize: 0,
		FileSize:  0,
		ModTime:   time.Time{},
		NumChunks: 0,
		Complete:  true,
	})
	c.Check(sz, check.Equals, int64(b.Len()))
	c.Check(mod, check.DeepEquals, now)
}

func (s *FileStorageServerSuite) TestFlushFails(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			flush: errors.New("flush error"),
		},
	}
	// Ensure no panic
	server.Flush("", "storageaddress")
}

func (s *FileStorageServerSuite) TestFlushOk(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{},
	}
	server.Flush("", "storageaddress")
}

func (s *FileStorageServerSuite) TestPutOpenErr(c *check.C) {
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStagingErr: errors.New("open staging error"),
		},
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.ErrorMatches, "open staging error")
}

func (s *FileStorageServerSuite) TestPutResolveErr(c *check.C) {
	f := &FakeFileIOFile{
		close: errors.New("close error"),
	}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging: f,
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", errors.New("resolver error")
	}
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.ErrorMatches, "resolver error")
}

func (s *FileStorageServerSuite) TestPutResolveErrPreserved(c *check.C) {
	f := &FakeFileIOFile{
		close: errors.New("close file error"),
	}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging: f,
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", errors.New("resolver error")
	}
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.ErrorMatches, "resolver error")
}

func (s *FileStorageServerSuite) TestPutMkDirError(c *check.C) {
	f := &FakeFileIOFile{}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging: f,
			mkdir:       errors.New("mkdir error"),
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "custom_dir", "storageaddress")
	c.Check(err, check.ErrorMatches, "mkdir error")
}

func (s *FileStorageServerSuite) TestPutMkDirErrorIgnored(c *check.C) {
	f := &FakeFileIOFile{}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging: f,
			mkdir:       errors.New("mkdir error"),
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	// Since the `dir` parameter is blank, we don't attempt to create a custom dir
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestPutMoveError(c *check.C) {
	f := &FakeFileIOFile{}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging:   f,
			stagingToPerm: errors.New("staging to perm error"),
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.ErrorMatches, "staging to perm error")
}

func (s *FileStorageServerSuite) TestPutOk(c *check.C) {
	f := &FakeFileIOFile{}
	ffi := &fakeFileIO{
		openStaging: f,
	}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO:      ffi,
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	d, a, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.IsNil)
	c.Check(ffi.permanent, check.Equals, "storageaddress")
	c.Check(d, check.Equals, "")
	c.Check(a, check.Equals, "storageaddress")
}

func (s *FileStorageServerSuite) TestPutChunked(c *check.C) {
	f := &servertest.DummyChunkUtils{
		WriteErr: errors.New("write error"),
	}
	server := &StorageServer{
		chunker: f,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}

	_, _, err := server.PutChunked(resolve, "", "", 0)
	c.Check(err, check.ErrorMatches, "cache only supports pre-addressed chunked put commands")

	_, _, err = server.PutChunked(resolve, "", "storageaddress", 0)
	c.Check(err, check.ErrorMatches, "cache only supports pre-sized chunked put commands")

	_, _, err = server.PutChunked(resolve, "", "storageaddress", 25)
	c.Check(err, check.ErrorMatches, "write error")

	f.WriteErr = nil
	d, a, err := server.PutChunked(resolve, "dir", "storageaddress", 25)
	c.Check(err, check.IsNil)
	c.Assert(d, check.Equals, "dir")
	c.Assert(a, check.Equals, "storageaddress")
}

func (s *FileStorageServerSuite) TestPutOkDeferredAddress(c *check.C) {
	f := &FakeFileIOFile{}
	ffi := &fakeFileIO{
		openStaging: f,
	}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO:      ffi,
		debugLogger: debugLogger,
	}
	// In this test, the resolver function returns the dir and address
	// for the item in the cache.
	resolve := func(w io.Writer) (string, string, error) {
		return "mydir", "deferred", nil
	}
	// Note that we don't provide a dir and address here since the
	// resolver will provide it instead.
	d, a, err := server.Put(resolve, "", "")
	c.Check(err, check.IsNil)
	c.Check(ffi.permanent, check.Equals, "mydir/deferred")
	c.Check(d, check.Equals, "mydir")
	c.Check(a, check.Equals, "deferred")
}

func (s *FileStorageServerSuite) TestPutOkCleanupFailure(c *check.C) {
	f := &FakeFileIOFile{}
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		fileIO: &fakeFileIO{
			openStaging: f,
			remove:      errors.New("remove error that isn't caught"),
		},
		debugLogger: debugLogger,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "", "storageaddress")
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestRemove(c *check.C) {
	now := time.Now()
	nowbytes, err := now.MarshalJSON()
	c.Assert(err, check.IsNil)
	info := []byte(fmt.Sprintf(`{"chunk_size":64,"file_size":3232,"num_chunks":15,"complete":true,"mod_time":%s}`, string(nowbytes)))
	buf := bytes.NewBuffer(info)
	f := &FakeFileIOFile{
		contents: buf,
	}
	stat := &fakeFileStat{
		dir: false,
	}
	fileIO := &fakeFileIO{
		openStaging: f,
		statErr:     errors.New("stat error"),
		stat:        stat,
		remove:      errors.New("remove error"),
		removeAll:   errors.New("remove all error"),
	}
	server := &StorageServer{
		fileIO: fileIO,
	}
	err = server.Remove("", "storageaddress")
	c.Check(err, check.ErrorMatches, "stat error")

	fileIO.statErr = nil
	err = server.Remove("", "storageaddress")
	c.Check(err, check.ErrorMatches, "remove error")

	fileIO.remove = nil
	err = server.Remove("", "storageaddress")
	c.Check(err, check.IsNil)

	buf = bytes.NewBuffer(info)
	f = &FakeFileIOFile{
		contents: buf,
	}
	fileIO.open = f
	stat.dir = true
	err = server.Remove("", "storageaddress")
	c.Check(err, check.ErrorMatches, "remove all error")

	buf = bytes.NewBuffer(info)
	f = &FakeFileIOFile{
		contents: buf,
	}
	fileIO.open = f
	fileIO.removeAll = nil
	err = server.Remove("", "storageaddress")
	c.Check(err, check.IsNil)
}

func (s *FileStorageServerSuite) TestUsage(c *check.C) {
	tempdir, err := ioutil.TempDir("", "")
	c.Assert(err, check.IsNil)
	defer os.RemoveAll(tempdir)

	fileContents1 := make([]byte, int64(10*datasize.KB))
	rand.Read(fileContents1)
	fileContents2 := make([]byte, int64(20*datasize.KB))
	rand.Read(fileContents2)
	fileContents3 := make([]byte, int64(30*datasize.KB))
	rand.Read(fileContents3)
	fileContents4 := make([]byte, int64(5*datasize.KB))
	rand.Read(fileContents4)
	fileContents5 := make([]byte, int64(15*datasize.KB))
	rand.Read(fileContents5)
	dir1, err := ioutil.TempDir(tempdir, "dutest")
	dir2, err := ioutil.TempDir(dir1, "another")
	defer os.RemoveAll(dir1)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(tempdir, "test1"), fileContents1, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir1, "test2"), fileContents2, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir1, "test3"), fileContents3, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir2, "test4"), fileContents4, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir2, "test5"), fileContents5, 0644)
	c.Assert(err, check.IsNil)

	server := &StorageServer{
		dir:          tempdir,
		debugLogger:  &servertest.TestLogger{},
		cacheTimeout: time.Minute,
	}
	fsUsage, err := server.CalculateUsage()
	c.Assert(err, check.IsNil)
	c.Assert(fsUsage.SizeBytes > datasize.ByteSize(0), check.Equals, true)
	c.Assert(fsUsage.FreeBytes > datasize.ByteSize(0), check.Equals, true)
	c.Assert(fsUsage.UsedBytes, check.Equals, 80*datasize.KB)
}

func (s *FileStorageServerSuite) TestUsageTimeout(c *check.C) {
	tempdir, err := ioutil.TempDir("", "")
	c.Assert(err, check.IsNil)
	defer os.RemoveAll(tempdir)

	fileContents1 := make([]byte, int64(10*datasize.KB))
	rand.Read(fileContents1)
	fileContents2 := make([]byte, int64(20*datasize.KB))
	rand.Read(fileContents2)
	fileContents3 := make([]byte, int64(30*datasize.KB))
	rand.Read(fileContents3)
	fileContents4 := make([]byte, int64(5*datasize.KB))
	rand.Read(fileContents4)
	fileContents5 := make([]byte, int64(15*datasize.KB))
	rand.Read(fileContents5)
	dir1, err := ioutil.TempDir(tempdir, "dutest")
	dir2, err := ioutil.TempDir(dir1, "another")
	defer os.RemoveAll(dir1)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(tempdir, "test1"), fileContents1, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir1, "test2"), fileContents2, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir1, "test3"), fileContents3, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir2, "test4"), fileContents4, 0644)
	c.Assert(err, check.IsNil)
	err = ioutil.WriteFile(filepath.Join(dir2, "test5"), fileContents5, 0644)
	c.Assert(err, check.IsNil)

	server := &StorageServer{
		dir:         tempdir,
		debugLogger: &servertest.TestLogger{},
	}
	fsUsage, err := server.CalculateUsage()
	c.Assert(err, check.NotNil)
	c.Assert(fsUsage.SizeBytes, check.Equals, datasize.ByteSize(0))
	c.Assert(fsUsage.FreeBytes, check.Equals, datasize.ByteSize(0))
	c.Assert(fsUsage.UsedBytes, check.Equals, 0*datasize.KB)
}

func (s *FileStorageServerSuite) TestDiskUsage(c *check.C) {
	testFiles := 10
	testStr := []byte("hello world")

	expectedSize := 0

	for i := 0; i < testFiles; i++ {
		f, _ := os.CreateTemp("testdata", "*")
		f.Write(testStr)
		expectedSize += len(testStr)

		defer os.Remove(f.Name())
	}

	sz, err := diskUsage("testdata", time.Minute, time.Second)
	c.Assert(err, check.IsNil)
	c.Check(uint64(sz), check.Equals, uint64(expectedSize))
}

func (s *FileStorageServerSuite) TestDiskUsageCacheTimeout(c *check.C) {
	testFiles := 10
	testStr := []byte("hello world")

	expectedSize := 0

	for i := 0; i < testFiles; i++ {
		f, _ := os.CreateTemp("testdata", "*")
		f.Write(testStr)
		expectedSize += len(testStr)

		defer os.Remove(f.Name())
	}

	_, err := diskUsage("testdata", time.Nanosecond, time.Minute)
	c.Assert(err, check.Equals, cacheTimeoutErr)
}

func (s *FileStorageServerSuite) TestDiskUsageWalkTimeout(c *check.C) {
	testFiles := 10
	testStr := []byte("hello world")

	expectedSize := 0

	for i := 0; i < testFiles; i++ {
		f, _ := os.CreateTemp("testdata", "*")
		f.Write(testStr)
		expectedSize += len(testStr)

		defer os.Remove(f.Name())
	}

	_, err := diskUsage("testdata", time.Minute, time.Nanosecond)
	c.Assert(err, check.Equals, walktimeoutErr)
}

var _ = check.Suite(&FileEnumerationSuite{})

type FileEnumerationSuite struct {
	tempDirHelper servertest.TempDirHelper
}

func (s *FileEnumerationSuite) SetUpTest(c *check.C) {
	s.tempDirHelper = servertest.TempDirHelper{}
	c.Assert(s.tempDirHelper.SetUp(), check.IsNil)
}

func (s *FileEnumerationSuite) TearDownTest(c *check.C) {
	c.Assert(s.tempDirHelper.TearDown(), check.IsNil)
}

func createTempFile(dir, file, data string, c *check.C) {
	err := os.MkdirAll(dir, 0700)
	c.Assert(err, check.IsNil)

	temp := filepath.Join(dir, file)
	t, err := os.Create(temp)
	c.Assert(err, check.IsNil)
	_, err = t.WriteString(data)
	c.Assert(err, check.IsNil)
	err = t.Close()
	c.Assert(err, check.IsNil)
}

func (s *FileEnumerationSuite) TestEnumerate(c *check.C) {
	debugLogger := &servertest.TestLogger{}
	server := &StorageServer{
		dir:         s.tempDirHelper.Dir(),
		fileIO:      &defaultFileIO{},
		debugLogger: debugLogger,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Create some files
	createTempFile(server.dir, "PACKAGES", "some data", c)
	createTempFile(filepath.Join(server.dir, "af"), "data.json", "{\"val\":\"test\"}", c)
	createTempFile(filepath.Join(server.dir, "af/test"), "data2.json", "{\"val\":\"test2\"}", c)

	// Create some chunked data
	resolve := func(w io.Writer) (string, string, error) {
		buf := bytes.NewBufferString(servertest.TestDESC)
		_, err := io.Copy(w, buf)
		return "", "", err
	}

	sz := uint64(len(servertest.TestDESC))
	_, _, err := server.PutChunked(resolve, "dir", "DESCRIPTION", sz)
	c.Check(err, check.IsNil)

	_, _, err = server.PutChunked(resolve, "", "DESCRIPTION2", sz)
	c.Check(err, check.IsNil)

	// Successfully enumerate files
	en, err := server.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "DESCRIPTION2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "DESCRIPTION",
			Chunked: true,
		},
		{
			Dir:     "",
			Address: "PACKAGES",
		},
		{
			Dir:     "af",
			Address: "data.json",
		},
		{
			Dir:     "af/test",
			Address: "data2.json",
		},
	})
}

func (s *FileEnumerationSuite) TestEnumerateWalkTimeout(c *check.C) {
	testFiles := 10
	testStr := []byte("hello world")

	expectedSize := 0

	for i := 0; i < testFiles; i++ {
		f, _ := os.CreateTemp("testdata", "*")
		f.Write(testStr)
		expectedSize += len(testStr)

		defer os.Remove(f.Name())
	}

	_, err := enumerate("testdata", time.Nanosecond)
	c.Assert(err, check.Equals, walktimeoutErr)
}

var _ = check.Suite(&FileCopyMoveSuite{})

type FileCopyMoveSuite struct {
	tempDirHelper servertest.TempDirHelper
}

func (s *FileCopyMoveSuite) SetUpTest(c *check.C) {
	s.tempDirHelper = servertest.TempDirHelper{}
	c.Assert(s.tempDirHelper.SetUp(), check.IsNil)
}

func (s *FileCopyMoveSuite) TearDownTest(c *check.C) {
	c.Assert(s.tempDirHelper.TearDown(), check.IsNil)
}

func (s *FileCopyMoveSuite) TestCopyFail(c *check.C) {
	f := &FakeFileIOFile{
		stat: &fakeFileStat{},
	}
	fi := &fakeFileIO{
		stat:    &fakeFileStat{},
		open:    f,
		openErr: errors.New("open error"),
	}
	serverSource := &StorageServer{
		fileIO: fi,
	}
	serverDest := &rsstorage.DummyStorageServer{
		PutErr: errors.New("put error"),
	}
	err := serverSource.Copy("dir", "address", serverDest)
	c.Assert(err, check.ErrorMatches, "open error")

	fi.openErr = nil
	err = serverSource.Copy("dir", "address", serverDest)
	c.Assert(err, check.ErrorMatches, "put error")
}

func (s *FileCopyMoveSuite) TestCopyReal(c *check.C) {
	// Create both destination and source directories
	dirSource, err := ioutil.TempDir(s.tempDirHelper.Dir(), "a")
	c.Assert(err, check.IsNil)
	dirDest, err := ioutil.TempDir(s.tempDirHelper.Dir(), "b")
	c.Assert(err, check.IsNil)

	// Associated a server with each directory (source and destination)
	debugLogger := &servertest.TestLogger{}
	serverSource := &StorageServer{
		dir:         dirSource,
		fileIO:      &defaultFileIO{},
		debugLogger: debugLogger,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	serverSource.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    serverSource,
		Waiter:    wn,
		Notifier:  wn,
	}
	serverDest := &StorageServer{
		dir:         dirDest,
		fileIO:      &defaultFileIO{},
		debugLogger: debugLogger,
	}
	serverDest.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    serverDest,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Create some files
	createTempFile(serverSource.dir, "PACKAGES", "some data", c)
	createTempFile(filepath.Join(serverSource.dir, "af"), "data.json", "{\"val\":\"test\"}", c)
	createTempFile(filepath.Join(serverSource.dir, "af/test"), "data2.json", "{\"val\":\"test2\"}", c)

	// Create some chunked data
	resolve := func(w io.Writer) (string, string, error) {
		buf := bytes.NewBufferString(servertest.TestDESC)
		_, err := io.Copy(w, buf)
		return "", "", err
	}
	sz := uint64(len(servertest.TestDESC))
	_, _, err = serverSource.PutChunked(resolve, "dir", "CHUNK", sz)
	c.Check(err, check.IsNil)
	_, _, err = serverSource.PutChunked(resolve, "", "CHUNK2", sz)
	c.Check(err, check.IsNil)

	// Successfully copy files
	err = serverSource.Copy("", "PACKAGES", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Copy("af", "data.json", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Copy("af/test", "data2.json", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Copy("dir", "CHUNK", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Copy("", "CHUNK2", serverDest)
	c.Assert(err, check.IsNil)

	// Successfully enumerate files
	en, err := serverDest.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNK2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNK",
			Chunked: true,
		},
		{
			Dir:     "",
			Address: "PACKAGES",
		},
		{
			Dir:     "af",
			Address: "data.json",
		},
		{
			Dir:     "af/test",
			Address: "data2.json",
		},
	})

	// Read one chunk
	f, _, _, _, _, err := serverDest.Get("", "CHUNK2")
	c.Assert(err, check.IsNil)
	b, err := ioutil.ReadAll(f)
	c.Assert(err, check.IsNil)
	c.Assert(string(b), check.Equals, servertest.TestDESC)

	// Files should still be on source
	en, err = serverSource.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNK2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNK",
			Chunked: true,
		},
		{
			Dir:     "",
			Address: "PACKAGES",
		},
		{
			Dir:     "af",
			Address: "data.json",
		},
		{
			Dir:     "af/test",
			Address: "data2.json",
		},
	})
}

func (s *FileCopyMoveSuite) TestMoveFail(c *check.C) {
	f := &FakeFileIOFile{
		contents: bytes.NewBufferString("test"),
		stat:     &fakeFileStat{},
	}
	fi := &fakeFileIO{
		stat:    &fakeFileStat{},
		open:    f,
		openErr: errors.New("open error"),
	}
	serverSource := &StorageServer{
		fileIO: fi,
	}
	serverDest := &rsstorage.DummyStorageServer{
		PutErr: errors.New("put error"),
	}

	// Since serverSource and serverDest are of the same type, we first
	// attempt to use `os.Rename` to move the file. Since the file doesn't exist,
	// we fall back to copying, which errors when the file can't be opened
	err := serverSource.Move("dir", "address", serverDest)
	c.Assert(err, check.ErrorMatches, "open error")

	// Next, we remove the open error, and fail on the `Put.
	fi.openErr = nil
	err = serverSource.Move("dir", "address", serverDest)
	c.Assert(err, check.ErrorMatches, "put error")

	// Now, we remove the put error, but fail on removing the file
	serverDest.PutErr = nil
	fi.remove = errors.New("remove error")
	err = serverSource.Move("dir", "address", serverDest)
	c.Assert(err, check.ErrorMatches, "remove error")

	// Now, we remove the put error, but fail on removing the file
	fi.remove = nil
	err = serverSource.Move("dir", "address", serverDest)
	c.Assert(err, check.IsNil)
}

func (s *FileCopyMoveSuite) TestMoveReal(c *check.C) {
	// Create both destination and source directories
	dirSource, err := ioutil.TempDir(s.tempDirHelper.Dir(), "a")
	c.Assert(err, check.IsNil)
	dirDest, err := ioutil.TempDir(s.tempDirHelper.Dir(), "b")
	c.Assert(err, check.IsNil)

	// Associated a server with each directory (source and destination)
	debugLogger := &servertest.TestLogger{}
	serverSource := &StorageServer{
		dir:         dirSource,
		fileIO:      &defaultFileIO{},
		debugLogger: debugLogger,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	serverSource.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    serverSource,
		Waiter:    wn,
		Notifier:  wn,
	}
	serverDest := &StorageServer{
		dir:         dirDest,
		fileIO:      &defaultFileIO{},
		debugLogger: debugLogger,
	}
	serverDest.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    serverDest,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Create some files
	createTempFile(serverSource.dir, "PACKAGES", "some data", c)
	createTempFile(filepath.Join(serverSource.dir, "af"), "data.json", "{\"val\":\"test\"}", c)
	createTempFile(filepath.Join(serverSource.dir, "af/test"), "data2.json", "{\"val\":\"test2\"}", c)

	// Create some chunked data
	resolve := func(w io.Writer) (string, string, error) {
		buf := bytes.NewBufferString(servertest.TestDESC)
		_, err := io.Copy(w, buf)
		return "", "", err
	}
	sz := uint64(len(servertest.TestDESC))
	_, _, err = serverSource.PutChunked(resolve, "dir", "CHUNK", sz)
	c.Check(err, check.IsNil)
	_, _, err = serverSource.PutChunked(resolve, "", "CHUNK2", sz)
	c.Check(err, check.IsNil)

	// Successfully move files
	err = serverSource.Move("", "PACKAGES", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Move("af", "data.json", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Move("af/test", "data2.json", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Move("dir", "CHUNK", serverDest)
	c.Assert(err, check.IsNil)
	err = serverSource.Move("", "CHUNK2", serverDest)
	c.Assert(err, check.IsNil)

	// Successfully enumerate files
	en, err := serverDest.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNK2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNK",
			Chunked: true,
		},
		{
			Dir:     "",
			Address: "PACKAGES",
		},
		{
			Dir:     "af",
			Address: "data.json",
		},
		{
			Dir:     "af/test",
			Address: "data2.json",
		},
	})

	// Read one chunk
	f, _, _, _, _, err := serverDest.Get("dir", "CHUNK")
	b, err := ioutil.ReadAll(f)
	c.Assert(err, check.IsNil)
	c.Assert(string(b), check.Equals, servertest.TestDESC)

	// Files should no longer be on source
	en, err = serverSource.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{})
}

func (s *FileCopyMoveSuite) TestLocate(c *check.C) {
	server := &StorageServer{
		dir: "/some/test/dir",
	}
	c.Check(server.Locate("dir", "address"), check.Equals, "/some/test/dir/dir/address")
	c.Check(server.Locate("", "address"), check.Equals, "/some/test/dir/address")
}
