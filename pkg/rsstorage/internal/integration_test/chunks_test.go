package integrationtest

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fortytw2/leaktest"
	"github.com/jackc/pgx/v4/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal/servertest"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/file"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/postgres"
	"github.com/rstudio/platform-lib/pkg/rsstorage/servers/s3server"
	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

type ChunksIntegrationSuite struct {
	pool          *pgxpool.Pool
	tempdirhelper servertest.TempDirHelper
}

var _ = check.Suite(&ChunksIntegrationSuite{})

func (s *ChunksIntegrationSuite) SetUpTest(c *check.C) {
	var err error
	dbname := strings.ToLower(internal.RandomString(16)) // databases must be lower case
	if !testing.Short() {
		s.pool, err = EphemeralPostgresPool(dbname)
		c.Assert(err, check.IsNil)
	}

	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *ChunksIntegrationSuite) TearDownTest(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}

// Creates a set of servers that cover all our supported storage subsystems.
// In the tests, we'll typically create two server sets, one set as a source
// set and another set as a destination set.
func (s *ChunksIntegrationSuite) NewServerSet(c *check.C, class, prefix string) map[string]rsstorage.StorageServer {
	s3Svc, err := s3server.NewS3Wrapper(&rsstorage.ConfigS3{
		Bucket:             class,
		Endpoint:           "http://minio:9000",
		Prefix:             prefix,
		EnableSharedConfig: true,
		DisableSSL:         true,
		S3ForcePathStyle:   true,
	}, "")
	c.Assert(err, check.IsNil)

	// Create S3 bucket
	if !testing.Short() {
		_, err = s3Svc.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(class),
		})
		c.Assert(err, check.IsNil)
	}

	// Prep store
	cstore := &dummyStore{
		pool: s.pool,
	}

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	debugLogger := &servertest.TestLogger{}
	pgServer := postgres.NewStorageServer(postgres.StorageServerArgs{
		ChunkSize:   100 * 1024,
		Waiter:      wn,
		Notifier:    wn,
		Class:       class,
		DebugLogger: debugLogger,
		Pool:        s.pool,
	})
	s3Server := s3server.NewStorageServer(s3server.StorageServerArgs{
		Bucket:    class,
		Svc:       s3Svc,
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
	})
	fileServer := file.NewStorageServer(file.StorageServerArgs{
		Dir:          dir,
		ChunkSize:    100 * 1024,
		Waiter:       wn,
		Notifier:     wn,
		Class:        "chunks",
		DebugLogger:  debugLogger,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})

	return map[string]rsstorage.StorageServer{
		"file": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "file",
			Server: fileServer,
			Store:  cstore,
		}),
		"s3": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "s3",
			Server: s3Server,
			Store:  cstore,
		}),
		"postgres": rsstorage.NewMetadataStorageServer(rsstorage.MetadataStorageServerArgs{
			Name:   "pg",
			Server: pgServer,
			Store:  cstore,
		}),
	}
}

// This test will only validate File storage when used without Postgres and MinIO. To test
// all services, use the `make test-integration` target. To run these tests only, use:
// `MODULE=pkg/rsstorage/internal/integration_test just test-integration -v github.com/rstudio/platform-lib/pkg/rsstorage/internal/integration_test -check.f=ChunksIntegrationSuite`
func (s *ChunksIntegrationSuite) TestWriteChunked(c *check.C) {
	serverSet := s.NewServerSet(c, "chunks", "")
	for key, server := range serverSet {
		if testing.Short() && key != "file" {
			log.Printf("skipping chunks integration tests for %s because -short was provided", key)
		} else {
			log.Printf("testing chunks integration tests for %s", key)
			s.check(c, server)
		}
	}
}

func (s *ChunksIntegrationSuite) check(c *check.C, chunkServer rsstorage.StorageServer) {

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}

	cw := &internal.DefaultChunkUtils{
		ChunkSize: 5,
		Server:    chunkServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Write some dummy data first to make sure it gets cleaned up
	zeros := strings.Repeat("0", 1000)
	sz := uint64(len(zeros))
	resolveJunk := func(writer io.Writer) (dir, address string, err error) {
		b := bytes.NewBufferString(zeros)
		_, err = io.Copy(writer, b)
		return
	}
	err := cw.WriteChunked("0a", "test-chunk", sz, resolveJunk)
	c.Assert(err, check.IsNil)

	sz = uint64(len(servertest.TestDESC))

	resolve := func(writer io.Writer) (dir, address string, err error) {
		b := bytes.NewBufferString(servertest.TestDESC)
		_, err = io.Copy(writer, b)
		return
	}

	err = cw.WriteChunked("0a", "test-chunk", sz, resolve)
	c.Assert(err, check.IsNil)

	// Look for expected files in output directory
	items, err := chunkServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Assert(items, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "0a",
			Address: "test-chunk",
			Chunked: true,
		},
	})

	// Verify info.json
	infoFile, _, _, _, ok, err := chunkServer.Get("0a/test-chunk", "info.json")
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
	info := types.ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	c.Assert(err, check.IsNil)
	c.Check(time.Now().Sub(info.ModTime).Minutes() < 2, check.Equals, true)
	info.ModTime = time.Time{}
	c.Assert(info, check.DeepEquals, types.ChunksInfo{
		ChunkSize: 5,
		NumChunks: 391,
		FileSize:  1953,
		Complete:  true,
	})

	// Read chunks and assemble them...
	r, _, size, mod, err := cw.ReadChunked("0a", "test-chunk")
	c.Assert(err, check.IsNil)
	c.Assert(size, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)
	defer r.Close()

	// ...while copying to buffer
	b := &bytes.Buffer{}
	n, err := io.Copy(b, r)
	c.Assert(err, check.IsNil)
	c.Assert(n, check.Equals, int64(1953))

	// Check SHA of assembled chunks
	sha := sha256.New()
	_, err = sha.Write(b.Bytes())
	c.Assert(err, check.IsNil)
	assembledChecksum := hex.EncodeToString(sha.Sum(nil))
	c.Assert(assembledChecksum, check.Equals, "b62eee53e57da53802a706f1ede904e0051675c981f9df5974b913bd76b1e8f8")
}

type ChunksPartialReadSuite struct {
	tempdirhelper servertest.TempDirHelper
}

var _ = check.Suite(&ChunksPartialReadSuite{})

func (s *ChunksPartialReadSuite) SetUpSuite(c *check.C) {
	c.Assert(s.tempdirhelper.SetUp(), check.IsNil)
}

func (s *ChunksPartialReadSuite) TearDownSuite(c *check.C) {
	c.Assert(s.tempdirhelper.TearDown(), check.IsNil)
}

func (s *ChunksPartialReadSuite) TestReadPartialOk(c *check.C) {
	log.Printf("Starting test %s", c.TestName())
	defer leaktest.Check(c)

	sz := uint64(len(servertest.TestDESC))
	b := bytes.NewBufferString(servertest.TestDESC)
	chunkSize := uint64(5)

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	debugLogger := &servertest.TestLogger{}
	fileServer := file.NewStorageServer(file.StorageServerArgs{
		Dir:          dir,
		ChunkSize:    100 * 1024,
		Waiter:       wn,
		Notifier:     wn,
		Class:        "chunks",
		DebugLogger:  debugLogger,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})

	cw := &internal.DefaultChunkUtils{
		ChunkSize:   chunkSize,
		Server:      fileServer,
		Waiter:      wn,
		Notifier:    wn,
		PollTimeout: rsstorage.DefaultChunkPollTimeout,
		MaxAttempts: rsstorage.DefaultMaxChunkAttempts,
	}

	// Slowly write to pipe
	pR, pW := io.Pipe()
	readStart := make(chan struct{})
	go func() {
		defer pW.Close()
		var index int
		for {
			_, err = io.CopyN(pW, b, int64(cw.ChunkSize))
			if err == io.EOF {
				err = nil
				return
			}
			c.Assert(err, check.IsNil)
			index++

			// After 100 chunks have been written, notify to start reading
			if index == 100 {
				close(readStart)
			}

			// On the last chunk, wait longer than usual
			if index == 390 {
				time.Sleep(time.Millisecond * 500)
			}
		}
	}()

	// Slowly resolve. Bytes will be copied to the pipe reader (pR) every ~10ms.
	resolve := func(writer io.Writer) (dir, address string, err error) {
		defer pR.Close()
		n, err := io.Copy(writer, pR)
		log.Printf("Done resolving %d bytes", n)
		return
	}

	// Start writing using the slow resolver
	go func() {
		err = cw.WriteChunked("0a", "test-chunk", sz, resolve)
		c.Assert(err, check.IsNil)
		log.Printf("Done with WriteChunked")
	}()

	// Wait for 100 chunks to be written
	<-readStart
	log.Printf("Starting ReadChunked after writing 100 chunks")

	// Read chunks and assemble them...
	r, _, size, mod, err := cw.ReadChunked("0a", "test-chunk")
	c.Assert(err, check.IsNil)
	c.Assert(size, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)
	defer r.Close()

	// ...while copying to buffer
	bOut := &bytes.Buffer{}
	n, err := io.Copy(bOut, r)
	c.Assert(err, check.IsNil)
	c.Assert(n, check.Equals, int64(1953))

	// Check SHA of assembled chunks
	sha := sha256.New()
	_, err = sha.Write(bOut.Bytes())
	c.Assert(err, check.IsNil)
	assembledChecksum := hex.EncodeToString(sha.Sum(nil))
	c.Assert(assembledChecksum, check.Equals, "b62eee53e57da53802a706f1ede904e0051675c981f9df5974b913bd76b1e8f8")
}

func (s *ChunksPartialReadSuite) TestReadPartialTimeout(c *check.C) {
	defer leaktest.Check(c)

	sz := uint64(len(servertest.TestDESC))
	b := bytes.NewBufferString(servertest.TestDESC)
	chunkSize := uint64(5)

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}

	debugLogger := &servertest.TestLogger{}
	fileServer := file.NewStorageServer(file.StorageServerArgs{
		Dir:          dir,
		ChunkSize:    100 * 1024,
		Waiter:       wn,
		Notifier:     wn,
		Class:        "chunks",
		DebugLogger:  debugLogger,
		CacheTimeout: time.Minute,
		WalkTimeout:  time.Minute,
	})

	cw := &internal.DefaultChunkUtils{
		ChunkSize:   chunkSize,
		Server:      fileServer,
		PollTimeout: time.Millisecond * 100,
		MaxAttempts: 10,
		Waiter:      wn,
		Notifier:    wn,
	}

	// Slowly write to pipe
	pR, pW := io.Pipe()
	readStart := make(chan struct{})
	go func() {
		defer pW.Close()
		var index int
		for {
			_, err := io.CopyN(pW, b, int64(cw.ChunkSize))
			if err == io.EOF {
				err = nil
				return
			}
			c.Assert(err, check.IsNil)
			index++

			// After 100 chunks have been written, notify to start reading
			if index == 100 {
				close(readStart)
			}

			// On chunk 105, hang
			if index == 105 {
				log.Printf("Hanging for 1 minute to simulate timeout")
				time.Sleep(time.Minute)
			}
		}
	}()

	// Slowly resolve. Bytes will be copied to the pipe reader (pR) every ~10ms.
	resolve := func(writer io.Writer) (dir, address string, err error) {
		defer pR.Close()
		n, err := io.Copy(writer, pR)
		log.Printf("Done resolving %d bytes", n)
		return
	}

	// Start writing using the slow resolver
	go func() {
		err = cw.WriteChunked("0a", "test-chunk", sz, resolve)
		c.Assert(err, check.IsNil)
		log.Printf("Done with WriteChunked")
	}()

	// Wait for 100 chunks to be written
	<-readStart
	log.Printf("Starting ReadChunked after writing 100 chunks")

	// Read chunks and assemble them...
	r, _, size, mod, err := cw.ReadChunked("0a", "test-chunk")
	c.Assert(err, check.IsNil)
	c.Assert(size, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)
	defer r.Close()

	// ...while copying to buffer, the operation will fail
	bOut := &bytes.Buffer{}
	_, err = io.Copy(bOut, r)
	c.Assert(err, check.DeepEquals, rsstorage.ErrNoChunk)
}
