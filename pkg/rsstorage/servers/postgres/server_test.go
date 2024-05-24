package postgres

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal"
	"github.com/rstudio/platform-lib/pkg/rsstorage/internal/servertest"
	"github.com/rstudio/platform-lib/pkg/rsstorage/types"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

// This suite will be skipped when running tests with SQLite only. To test, use
// the `make test-integration` target. To run these tests only, use
// `MODULE=pkg/rsstorage just test-integration -v github.com/rstudio/platform-lib/pkg/rsstorage -check.f=PgCacheServerSuite`
type PgCacheServerSuite struct {
	pool *pgxpool.Pool
}

var _ = check.Suite(&PgCacheServerSuite{})

func (s *PgCacheServerSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping postgres cache server tests because -short was provided")
	}
	rand.Seed(time.Now().UnixNano())
}

func (s *PgCacheServerSuite) SetUpTest(c *check.C) {
	var err error
	dbname := strings.ToLower(internal.RandomString(16)) // databases must be lower case
	s.pool, err = EphemeralPostgresPool(dbname)
	c.Assert(err, check.IsNil)
	c.Assert(s.pool, check.NotNil)
}

func (s *PgCacheServerSuite) TestNew(c *check.C) {
	wn := &servertest.DummyWaiterNotifier{}
	server := NewStorageServer(StorageServerArgs{
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
		Class:     "test",
		Pool:      s.pool,
	})
	c.Check(server.(*StorageServer).chunker, check.NotNil)
	server.(*StorageServer).chunker = nil
	c.Check(server, check.DeepEquals, &StorageServer{
		pool:  s.pool,
		class: "test",
	})

	c.Assert(server.Dir(), check.Equals, "pg:test")
	c.Assert(server.Type(), check.Equals, rsstorage.StorageTypePostgres)
}

func (s *PgCacheServerSuite) TestCheckOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.Put(resolve, "dir", "cacheaddress")
	c.Check(err, check.IsNil)

	// Next, get it
	ok, chunked, sz, _, err := server.Check("dir", "cacheaddress")
	c.Check(err, check.IsNil)
	c.Assert(chunked, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(14))
}

func (s *PgCacheServerSuite) TestCheckChunkedOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 320,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte(servertest.TestDESC))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.PutChunked(resolve, "dir", "cacheaddress", uint64(len(servertest.TestDESC)))
	c.Assert(err, check.IsNil)

	// Next, get it
	ok, chunked, sz, mod, err := server.Check("dir", "cacheaddress")
	c.Assert(err, check.IsNil)
	c.Assert(chunked, check.NotNil)
	c.Assert(ok, check.Equals, true)
	c.Assert(sz, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)
}

func (s *PgCacheServerSuite) TestGetOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.Put(resolve, "dir", "cacheaddress")
	c.Check(err, check.IsNil)

	// Next, get it
	r, ch, sz, _, ok, err := server.Get("dir", "cacheaddress")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Assert(ch, check.IsNil)
	c.Check(sz, check.Equals, int64(14))

	// Check contents
	bs := bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, "this is a test")

	// Close it
	c.Assert(r.Close(), check.IsNil)
}

func (s *PgCacheServerSuite) TestGetChunkedOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 512,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte(servertest.TestDESC))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.PutChunked(resolve, "dir", "cacheaddress", uint64(len(servertest.TestDESC)))
	c.Assert(err, check.IsNil)

	// Next, get it
	r, ch, sz, mod, ok, err := server.Get("dir", "cacheaddress")
	c.Assert(err, check.IsNil)
	c.Assert(r, check.NotNil)
	c.Assert(ok, check.Equals, true)
	c.Assert(ch.ModTime.IsZero(), check.Equals, false)
	ch.ModTime = time.Time{}
	c.Assert(ch, check.DeepEquals, &types.ChunksInfo{
		ChunkSize: 512,
		FileSize:  1953,
		NumChunks: 4,
		Complete:  true,
	})
	c.Assert(sz, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)

	// Check contents
	bs := bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Assert(bs.String(), check.Equals, servertest.TestDESC)

	// Close it
	c.Assert(r.Close(), check.IsNil)
}

func (s *PgCacheServerSuite) TestPutResolveErr(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", errors.New("resolver error")
	}
	_, _, err := server.Put(resolve, "", "cacheaddress")
	c.Check(err, check.ErrorMatches, "resolver error")
}

func (s *PgCacheServerSuite) TestPutResolveErrPreserved(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", errors.New("resolver error")
	}
	_, _, err := server.Put(resolve, "", "cacheaddress")
	c.Check(err, check.ErrorMatches, "resolver error")
}

func (s *PgCacheServerSuite) TestPutOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "", "", err
	}

	// First, cache something
	d, a, err := server.Put(resolve, "adir", "cacheaddress")
	c.Check(err, check.IsNil)
	c.Check(d, check.Equals, "adir")
	c.Check(a, check.Equals, "cacheaddress")
}

func (s *PgCacheServerSuite) TestPutDeferredAddressOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "dir", "address", err
	}

	// First, cache something
	d, a, err := server.Put(resolve, "", "")
	c.Check(err, check.IsNil)
	c.Check(d, check.Equals, "dir")
	c.Check(a, check.Equals, "address")
}

func (s *PgCacheServerSuite) TestRemoveNonExisting(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	err := server.Remove("", "cacheaddress")
	c.Check(err, check.IsNil)
}

func (s *PgCacheServerSuite) TestRemoveOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}

	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.Put(resolve, "", "cacheaddress")
	c.Check(err, check.IsNil)

	err = server.Remove("", "cacheaddress")
	c.Check(err, check.IsNil)
}

func (s *PgCacheServerSuite) TestRemoveChunkedOk(c *check.C) {
	server := &StorageServer{
		pool: s.pool,
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	server.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 512,
		Server:    server,
		Waiter:    wn,
		Notifier:  wn,
	}
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte(servertest.TestDESC))
		return "", "", err
	}

	// First, cache something
	_, _, err := server.PutChunked(resolve, "some", "cacheaddress", uint64(len(servertest.TestDESC)))
	c.Assert(err, check.IsNil)

	// Item should be here
	ok, chunked, sz, mod, err := server.Check("some", "cacheaddress")
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, true)
	c.Assert(chunked, check.NotNil)
	c.Assert(sz, check.Equals, int64(1953))
	c.Assert(mod.IsZero(), check.Equals, false)

	err = server.Remove("some", "cacheaddress")
	c.Assert(err, check.IsNil)

	// Item should be gone
	ok, _, _, _, err = server.Check("some", "cacheaddress")
	c.Assert(err, check.IsNil)
	c.Assert(ok, check.Equals, false)
}

func put(server *StorageServer, dir, address string, c *check.C) {
	resolve := func(w io.Writer) (string, string, error) {
		_, err := w.Write([]byte("this is a test"))
		return "", "", err
	}

	// Cache the item
	_, _, err := server.Put(resolve, dir, address)
	c.Check(err, check.IsNil)
}

func (s *PgCacheServerSuite) TestEnumerate(c *check.C) {
	server := &StorageServer{
		pool:  s.pool,
		class: "cache",
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

	// First, cache some things
	put(server, "", "cacheaddress", c)
	put(server, "ad1", "cacheaddress2", c)
	put(server, "ad1", "cacheaddress3", c)
	put(server, "ad2", "cacheaddress4", c)
	put(server, "ad2/3", "cacheaddress5", c)
	put(server, "ad3/4", "cacheaddress6", c)

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
			Dir:     "ad1",
			Address: "cacheaddress2",
		},
		{
			Dir:     "ad1",
			Address: "cacheaddress3",
		},
		{
			Dir:     "ad2/3",
			Address: "cacheaddress5",
		},
		{
			Dir:     "ad2",
			Address: "cacheaddress4",
		},
		{
			Dir:     "ad3/4",
			Address: "cacheaddress6",
		},
		{
			Dir:     "",
			Address: "cacheaddress",
		},
	})
}

func (s *PgCacheServerSuite) TestCopy(c *check.C) {
	sourceServer := &StorageServer{
		pool:  s.pool,
		class: "packages",
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	sourceServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    sourceServer,
		Waiter:    wn,
		Notifier:  wn,
	}
	destServer := &StorageServer{
		pool:  s.pool,
		class: "cran",
	}
	destServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    destServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Create some chunked data
	resolve := func(w io.Writer) (string, string, error) {
		buf := bytes.NewBufferString(servertest.TestDESC)
		_, err := io.Copy(w, buf)
		return "", "", err
	}
	szPut := uint64(len(servertest.TestDESC))
	_, _, err := sourceServer.PutChunked(resolve, "dir", "CHUNKED", szPut)
	c.Check(err, check.IsNil)
	_, _, err = sourceServer.PutChunked(resolve, "", "CHUNKED2", szPut)
	c.Check(err, check.IsNil)

	put(sourceServer, "ad2", "cacheaddress4", c)
	put(sourceServer, "ad2/3", "cacheaddress5", c)

	// Attempt to copy an item that does not exist
	err = sourceServer.Copy("", "no-exist", destServer)
	c.Assert(err, check.ErrorMatches, "the PostgreSQL large object .* does not exist")

	// Copy
	err = sourceServer.Copy("ad2", "cacheaddress4", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Copy("ad2/3", "cacheaddress5", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Copy("dir", "CHUNKED", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Copy("", "CHUNKED2", destServer)
	c.Assert(err, check.IsNil)

	// Original items still exist
	en, err := sourceServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNKED2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNKED",
			Chunked: true,
		},
		{
			Dir:     "ad2/3",
			Address: "cacheaddress5",
		},
		{
			Dir:     "ad2",
			Address: "cacheaddress4",
		},
	})

	// New (copied) items exist
	en, err = destServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNKED2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNKED",
			Chunked: true,
		},
		{
			Dir:     "ad2/3",
			Address: "cacheaddress5",
		},
		{
			Dir:     "ad2",
			Address: "cacheaddress4",
		},
	})

	// Make sure we can get old item
	r, _, sz, _, ok, err := sourceServer.Get("ad2", "cacheaddress4")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(14))
	//
	// Check contents
	bs := bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, "this is a test")
	//
	// Close it
	c.Assert(r.Close(), check.IsNil)

	// Make sure we can get new item
	r, _, sz, _, ok, err = destServer.Get("ad2", "cacheaddress4")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(14))
	//
	// Check contents
	bs = bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, "this is a test")
	//
	// Close it
	c.Assert(r.Close(), check.IsNil)

	// Make sure we can get new chunked item
	r, _, sz, _, ok, err = destServer.Get("dir", "CHUNKED")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(1953))
	//
	// Check contents
	bs = bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, servertest.TestDESC)
	//
	// Close it
	c.Assert(r.Close(), check.IsNil)
}

func (s *PgCacheServerSuite) TestMove(c *check.C) {
	sourceServer := &StorageServer{
		pool:  s.pool,
		class: "packages",
	}
	wn := &servertest.DummyWaiterNotifier{
		Ch: make(chan bool, 1),
	}
	sourceServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    sourceServer,
		Waiter:    wn,
		Notifier:  wn,
	}
	destServer := &StorageServer{
		pool:  s.pool,
		class: "cran",
	}
	destServer.chunker = &internal.DefaultChunkUtils{
		ChunkSize: 352,
		Server:    destServer,
		Waiter:    wn,
		Notifier:  wn,
	}

	// Create some chunked data
	resolve := func(w io.Writer) (string, string, error) {
		buf := bytes.NewBufferString(servertest.TestDESC)
		_, err := io.Copy(w, buf)
		return "", "", err
	}
	szPut := uint64(len(servertest.TestDESC))
	_, _, err := sourceServer.PutChunked(resolve, "dir", "CHUNKED", szPut)
	c.Check(err, check.IsNil)
	_, _, err = sourceServer.PutChunked(resolve, "", "CHUNKED2", szPut)
	c.Check(err, check.IsNil)

	put(sourceServer, "ad2", "cacheaddress4", c)
	put(sourceServer, "ad2/3", "cacheaddress5", c)

	// Copy
	err = sourceServer.Move("ad2", "cacheaddress4", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Move("ad2/3", "cacheaddress5", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Move("dir", "CHUNKED", destServer)
	c.Assert(err, check.IsNil)
	err = sourceServer.Move("", "CHUNKED2", destServer)
	c.Assert(err, check.IsNil)

	// Original items no longer exist
	en, err := sourceServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{})

	// New (copied) items exist
	en, err = destServer.Enumerate()
	c.Assert(err, check.IsNil)
	c.Check(en, check.DeepEquals, []types.StoredItem{
		{
			Dir:     "",
			Address: "CHUNKED2",
			Chunked: true,
		},
		{
			Dir:     "dir",
			Address: "CHUNKED",
			Chunked: true,
		},
		{
			Dir:     "ad2/3",
			Address: "cacheaddress5",
		},
		{
			Dir:     "ad2",
			Address: "cacheaddress4",
		},
	})

	// Make sure we cannot get old item
	r, _, sz, _, ok, err := sourceServer.Get("ad2", "cacheaddress4")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(r, check.IsNil)

	// Make sure we cannot get old item
	r, _, sz, _, ok, err = sourceServer.Get("dir", "CHUNKED")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, false)
	c.Check(r, check.IsNil)

	// Make sure we can get new item
	r, _, sz, _, ok, err = destServer.Get("ad2", "cacheaddress4")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(14))
	//
	// Check contents
	bs := bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, "this is a test")
	//
	// Close it
	c.Assert(r.Close(), check.IsNil)

	// Make sure we can get new chunked item
	r, _, sz, _, ok, err = destServer.Get("dir", "CHUNKED")
	c.Check(err, check.IsNil)
	c.Check(ok, check.Equals, true)
	c.Check(sz, check.Equals, int64(1953))
	//
	// Check contents
	bs = bytes.NewBufferString("")
	_, err = io.Copy(bs, r)
	c.Assert(err, check.IsNil)
	c.Check(bs.String(), check.Equals, servertest.TestDESC)
	//
	// Close it
	c.Assert(r.Close(), check.IsNil)
}

func (s *PgCacheServerSuite) TestLocate(c *check.C) {
	server := &StorageServer{
		class: "storage-class",
	}
	c.Check(server.Locate("dir", "address"), check.Equals, "storage-class/dir/address")
	c.Check(server.Locate("", "address"), check.Equals, "storage-class/address")
}

func (s *PgCacheServerSuite) TestUsage(c *check.C) {
	wn := &servertest.DummyWaiterNotifier{}
	server := NewStorageServer(StorageServerArgs{
		ChunkSize: 100 * 1024,
		Waiter:    wn,
		Notifier:  wn,
		Class:     "testclass",
	})

	c.Assert(server.Dir(), check.Equals, "pg:testclass")
	c.Assert(server.Type(), check.Equals, rsstorage.StorageTypePostgres)
	usage, err := server.CalculateUsage()
	c.Assert(usage, check.DeepEquals, types.Usage{})
	c.Assert(err, check.NotNil)
}

func create(dbname string) (err error) {
	if dbname != "postgres" {
		connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", "postgres")

		var conn *pgx.Conn
		ctx := context.Background()
		conn, err = pgx.Connect(ctx, connectionString)
		if err != nil {
			return
		}
		defer conn.Close(ctx)

		_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbname))
	}
	return
}

func EphemeralPostgresPool(dbname string) (pool *pgxpool.Pool, err error) {
	err = create(dbname)
	if err != nil {
		return
	}

	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return
	}

	config.MaxConns = int32(10)

	pool, err = pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return
	}

	sql := "" +
		"CREATE TABLE large_objects ( " +
		"	oid INTEGER PRIMARY KEY, " +
		"	address TEXT UNIQUE NOT NULL " +
		");"
	_, err = pool.Exec(context.Background(), sql)

	return
}
