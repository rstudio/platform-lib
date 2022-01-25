package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
	"gopkg.in/check.v1"
)

type GetResult struct {
	GetReader  io.ReadCloser
	GetOk      bool
	GetChunked *ChunksInfo
	GetSize    int64
	GetModTime time.Time
	GetErr     error
}

type DummyPersistentStorageServer struct {
	GetAttempts    int
	GetReader      io.ReadCloser
	GetOk          bool
	GetChunked     *ChunksInfo
	GetSize        int64
	GetModTime     time.Time
	GetErr         error
	GetMap         map[string]GetResult
	RemoveErr      error
	RemoveMap      map[string]bool
	RemoveCount    int
	Flushed        int
	PutDelay       time.Duration
	PutErr         error
	PutCalled      int
	PutChunks      bool
	Address        []string
	Placed         []string
	Buffer         *bytes.Buffer
	EnumItems      []PersistentStorageItem
	EnumErr        error
	MoveErr        error
	Moved          []string
	CopyErr        error
	Copied         []string
	Location       string
	MockType       StorageType
	MockDir        string
	MockUsage      types.Usage
	MockUsageError error

	GetCheckLock sync.RWMutex
}

func (f *DummyPersistentStorageServer) Check(dir, address string) (bool, *ChunksInfo, int64, time.Time, error) {
	f.GetCheckLock.RLock()
	defer f.GetCheckLock.RUnlock()

	f.GetAttempts++
	if f.GetMap != nil {
		return f.GetMap[address].GetOk, f.GetMap[address].GetChunked, f.GetMap[address].GetSize, f.GetMap[address].GetModTime, f.GetMap[address].GetErr
	} else {
		return f.GetOk, f.GetChunked, f.GetSize, f.GetModTime, f.GetErr
	}
}

func (s *DummyPersistentStorageServer) Dir() string {
	return s.MockDir
}

func (s *DummyPersistentStorageServer) Type() StorageType {
	if s.MockType != StorageType("") {
		return s.MockType
	}
	return StorageTypePostgres
}

func (s *DummyPersistentStorageServer) CalculateUsage() (types.Usage, error) {
	return s.MockUsage, s.MockUsageError
}

func (f *DummyPersistentStorageServer) Get(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, bool, error) {
	f.GetCheckLock.RLock()
	defer f.GetCheckLock.RUnlock()

	f.GetAttempts++
	if f.GetMap != nil {
		return f.GetMap[address].GetReader, f.GetMap[address].GetChunked, f.GetMap[address].GetSize, f.GetMap[address].GetModTime, f.GetMap[address].GetOk, f.GetMap[address].GetErr
	} else {
		return f.GetReader, f.GetChunked, f.GetSize, f.GetModTime, f.GetOk, f.GetErr
	}
}

func (f *DummyPersistentStorageServer) Put(resolve Resolver, dir, address string) (string, string, error) {
	if f.PutDelay > 0 {
		time.Sleep(f.PutDelay)
	}
	f.PutCalled++
	if f.PutErr != nil {
		return "", "", f.PutErr
	} else {
		f.Buffer = bytes.NewBuffer([]byte{})
		d, a, err := resolve(f.Buffer)
		if dir == "" && address == "" {
			dir = d
			address = a
		}
		addressAndOutput := fmt.Sprintf("%s-%s", dir, f.Buffer.String())
		f.Address = append(f.Address, address)
		f.Placed = append(f.Placed, addressAndOutput)
		return dir, address, err
	}
}

func (f *DummyPersistentStorageServer) PutChunked(resolve Resolver, dir, address string, sz uint64) (string, string, error) {
	f.PutChunks = true
	return f.Put(resolve, dir, address)
}

func (f *DummyPersistentStorageServer) Remove(dir, address string) error {
	if f.RemoveMap == nil {
		f.RemoveMap = make(map[string]bool)
	}
	if f.RemoveErr == nil {
		f.RemoveCount++
		f.RemoveMap[dir+"/"+address] = true
	}
	return f.RemoveErr
}

func (f *DummyPersistentStorageServer) Flush(dir, address string) {
	f.Flushed++
}

func (f *DummyPersistentStorageServer) Enumerate() ([]PersistentStorageItem, error) {
	return f.EnumItems, f.EnumErr
}

func (f *DummyPersistentStorageServer) Move(dir, address string, server PersistentStorageServer) error {
	if f.MoveErr == nil && f.Moved != nil {
		f.Moved = append(f.Moved, dir+"/"+address)
	}
	return f.MoveErr
}

func (f *DummyPersistentStorageServer) Copy(dir, address string, server PersistentStorageServer) error {
	if f.CopyErr == nil && f.Copied != nil {
		f.Copied = append(f.Copied, dir+"/"+address)
	}
	return f.CopyErr
}

func (f *DummyPersistentStorageServer) Locate(dir, address string) string {
	return f.Location
}

func (f *DummyPersistentStorageServer) Base() PersistentStorageServer {
	return f
}

type DummyChunkUtils struct {
	WriteErr error
	Read     io.ReadCloser
	ReadCh   *ChunksInfo
	ReadSz   int64
	ReadMod  time.Time
	ReadErr  error
}

func (f *DummyChunkUtils) WriteChunked(dir, address string, sz uint64, resolve Resolver) error {
	return f.WriteErr
}

func (f *DummyChunkUtils) ReadChunked(dir, address string) (io.ReadCloser, *ChunksInfo, int64, time.Time, error) {
	return f.Read, f.ReadCh, f.ReadSz, f.ReadMod, f.ReadErr
}

type TestLogger struct {
	enabled bool
}

func (l *TestLogger) Debugf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (l *TestLogger) Enabled() bool {
	return l.enabled
}

type DummyWaiterNotifier struct {
	Ch chan bool
}

func (d *DummyWaiterNotifier) WaitForChunk(c *types.ChunkNotification) {
	to := time.NewTimer(time.Second)
	defer to.Stop()
	select {
	case <-d.Ch:
	case <-to.C:
	}
}

func (d *DummyWaiterNotifier) Notify(c *types.ChunkNotification) error {
	select {
	case d.Ch <- true:
	default:
	}
	return nil
}

type timeEquals struct {
	*check.CheckerInfo
}

// TimeEquals is a checker that uses time.Time.Equal to compare time.Time objects.
var TimeEquals check.Checker = &timeEquals{
	&check.CheckerInfo{Name: "TimeEquals", Params: []string{"obtained", "expected"}},
}

func (checker *timeEquals) Check(params []interface{}, names []string) (result bool, error string) {
	if obtained, ok := params[0].(time.Time); !ok {
		return false, "obtained is not a time.Time"
	} else if expected, ok := params[1].(time.Time); !ok {
		return false, "expected is not a time.Time"
	} else {
		// We cannot do a DeepEquals on Time because Time is not
		// comparable through reflection.
		return obtained.Unix() == expected.Unix(), ""
	}
}

// TempDirHelper helps tests create and destroy temporary directories.
type TempDirHelper struct {
	prefix string
	dir    string
}

// SetUp creates a temporary directory
func (h *TempDirHelper) SetUp() error {
	var err error
	h.dir, err = ioutil.TempDir("", h.prefix)
	return err
}

// TearDown removes the configured directory
func (h *TempDirHelper) TearDown() error {
	var err error
	if h.dir != "" {
		err = os.RemoveAll(h.dir)
		h.dir = ""
	}
	return err
}

// Dir returns the path to the configured directory
func (h *TempDirHelper) Dir() string {
	return h.dir
}
