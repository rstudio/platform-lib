package storage

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

	"rspm/storage/types"
)

var testDESC = `Encoding: UTF-8
Package: plumber
Type: Package
Title: An API Generator for R
Version: 0.4.2
Date: 2017-07-24
Authors@R: c(
  person(family="Trestle Technology, LLC", role="aut", email="cran@trestletech.com"),
  person("Jeff", "Allen", role="cre", email="cran@trestletech.com"),
  person("Frans", "van Dunné", role="ctb", email="frans@ixpantia.com"),
  person(family="SmartBear Software", role=c("ctb", "cph"), comment="swagger-ui"))
License: MIT + file LICENSE
BugReports: https://github.com/trestletech/plumber/issues
URL: https://www.rplumber.io (site)
        https://github.com/trestletech/plumber (dev)
Description: Gives the ability to automatically generate and serve an HTTP API
    from R functions using the annotations in the R documentation around your
    functions.
Depends: R (>= 3.0.0)
Imports: R6 (>= 2.0.0), stringi (>= 0.3.0), jsonlite (>= 0.9.16),
        httpuv (>= 1.2.3), crayon
LazyData: TRUE
Suggests: testthat (>= 0.11.0), XML, rmarkdown, PKI, base64enc,
        htmlwidgets, visNetwork, analogsea
LinkingTo: testthat (>= 0.11.0), XML, rmarkdown
Enhances: testthat (>= 0.12.0), XML, rmarkdown
Collate: 'content-types.R' 'cookie-parser.R' 'parse-globals.R'
        'images.R' 'parse-block.R' 'globals.R' 'serializer-json.R'
        'shared-secret-filter.R' 'post-body.R' 'query-string.R'
        'plumber.R' 'default-handlers.R' 'digital-ocean.R'
        'find-port.R' 'includes.R' 'paths.R' 'plumber-static.R'
        'plumber-step.R' 'response.R' 'serializer-content-type.R'
        'serializer-html.R' 'serializer-htmlwidget.R'
        'serializer-xml.R' 'serializer.R' 'session-cookie.R'
        'swagger.R'
RoxygenNote: 6.0.1
NeedsCompilation: no
Packaged: 2017-07-24 17:17:15 UTC; jeff
Author: Trestle Technology, LLC [aut],
  Jeff Allen [cre],
  Frans van Dunné [ctb],
  SmartBear Software [ctb, cph] (swagger-ui)
Maintainer: Jeff Allen <cran@trestletech.com>
Repository: CRAN
Date/Publication: 2017-07-24 21:50:56 UTC
`

type dummyWaiterNotifier struct {
	ch chan bool
}

func (d *dummyWaiterNotifier) WaitForChunk(c *types.ChunkNotification) {
	to := time.NewTimer(time.Second)
	defer to.Stop()
	select {
	case <-d.ch:
	case <-to.C:
	}
}

func (d *dummyWaiterNotifier) Notify(c *types.ChunkNotification) error {
	select {
	case d.ch <- true:
	default:
	}
	return nil
}

type ChunksIntegrationSuite struct {
	pool          *pgxpool.Pool
	tempdirhelper TempDirHelper
}

var _ = check.Suite(&ChunksIntegrationSuite{})

func (s *ChunksIntegrationSuite) SetUpTest(c *check.C) {
	var err error
	dbname := strings.ToLower(RandomString(16)) // databases must be lower case
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
func (s *ChunksIntegrationSuite) NewServerSet(c *check.C, class, prefix string) map[string]PersistentStorageServer {
	s3Svc, err := newS3Service(&ConfigS3{
		Bucket:             class,
		Endpoint:           "http://minio:9000",
		Prefix:             prefix,
		EnableSharedConfig: true,
		DisableSSL:         true,
		S3ForcePathStyle:   true,
	})
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

	wn := &dummyWaiterNotifier{
		ch: make(chan bool, 1),
	}
	debugLogger := &TestLogger{}
	pgServer := NewPgServer(class, 100*1024, wn, wn, cstore, debugLogger)
	s3Server := NewS3StorageServer(class, "", s3Svc, 100*1024, wn, wn)
	fileServer := NewFileStorageServer(dir, 100*1024, wn, wn, "chunks", debugLogger, time.Minute)

	return map[string]PersistentStorageServer{
		"file":     NewMetadataPersistentStorageServer("file", fileServer, cstore),
		"s3":       NewMetadataPersistentStorageServer("s3", s3Server, cstore),
		"postgres": NewMetadataPersistentStorageServer("pg", pgServer, cstore),
	}
}

// This test will only validate File storage when using without Postgres and MinIO. To test
// all services, use the `make test-integration` target. To run these tests only, use:
// `make test-integration TEST=rspm/storage TEST_ARGS="-v -check.f=ChunksIntegrationSuite"`
func (s *ChunksIntegrationSuite) TestWriteChunked(c *check.C) {
	serverSet := s.NewServerSet(c, "chunks", "")
	for key, server := range serverSet {
		if testing.Short() && key != "file" {
			log.Printf("skipping persistent storage chunks integration tests for %s because -short was provided", key)
		} else {
			log.Printf("testing persistent storage chunks integration tests for %s", key)
			s.check(c, server)
		}
	}
}

func (s *ChunksIntegrationSuite) check(c *check.C, chunkServer PersistentStorageServer) {

	wn := &dummyWaiterNotifier{
		ch: make(chan bool, 1),
	}

	cw := &defaultChunkUtils{
		chunkSize: 5,
		server:    chunkServer,
		waiter:    wn,
		notifier:  wn,
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

	sz = uint64(len(testDESC))

	resolve := func(writer io.Writer) (dir, address string, err error) {
		b := bytes.NewBufferString(testDESC)
		_, err = io.Copy(writer, b)
		return
	}

	err = cw.WriteChunked("0a", "test-chunk", sz, resolve)
	c.Assert(err, check.IsNil)

	// Look for expected files in output directory
	items, err := chunkServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Assert(items, check.DeepEquals, []PersistentStorageItem{
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
	info := ChunksInfo{}
	dec := json.NewDecoder(infoFile)
	err = dec.Decode(&info)
	c.Assert(err, check.IsNil)
	c.Check(time.Now().Sub(info.ModTime).Minutes() < 2, check.Equals, true)
	info.ModTime = time.Time{}
	c.Assert(info, check.DeepEquals, ChunksInfo{
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
	tempdirhelper TempDirHelper
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

	sz := uint64(len(testDESC))
	b := bytes.NewBufferString(testDESC)
	chunkSize := uint64(5)

	wn := &dummyWaiterNotifier{
		ch: make(chan bool, 1),
	}

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	debugLogger := &TestLogger{}
	fileServer := NewFileStorageServer(dir, 100*1024, wn, wn, "chunks", debugLogger, time.Minute)

	cw := &defaultChunkUtils{
		chunkSize:   chunkSize,
		server:      fileServer,
		waiter:      wn,
		notifier:    wn,
		pollTimeout: chunkPollTimeout,
		maxAttempts: maxChunkAttempts,
	}

	// Slowly write to pipe
	pR, pW := io.Pipe()
	readStart := make(chan struct{})
	go func() {
		defer pW.Close()
		var index int
		for {
			_, err = io.CopyN(pW, b, int64(cw.chunkSize))
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

	sz := uint64(len(testDESC))
	b := bytes.NewBufferString(testDESC)
	chunkSize := uint64(5)

	// Prep directory for file storage
	dir, err := ioutil.TempDir(s.tempdirhelper.Dir(), "")
	c.Assert(err, check.IsNil)

	wn := &dummyWaiterNotifier{
		ch: make(chan bool, 1),
	}

	debugLogger := &TestLogger{}
	fileServer := NewFileStorageServer(dir, 100*1024, wn, wn, "chunks", debugLogger, time.Minute)

	cw := &defaultChunkUtils{
		chunkSize:   chunkSize,
		server:      fileServer,
		pollTimeout: time.Millisecond * 100,
		maxAttempts: 10,
		waiter:      wn,
		notifier:    wn,
	}

	// Slowly write to pipe
	pR, pW := io.Pipe()
	readStart := make(chan struct{})
	go func() {
		defer pW.Close()
		var index int
		for {
			_, err := io.CopyN(pW, b, int64(cw.chunkSize))
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
	c.Assert(err, check.DeepEquals, ErrNoChunk)
}
