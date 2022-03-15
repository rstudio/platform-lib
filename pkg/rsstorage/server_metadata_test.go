package rsstorage

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"

	"gopkg.in/check.v1"
)

type MetadataServerSuite struct{}

var _ = check.Suite(&MetadataServerSuite{})

type cacheStore struct {
	getErr error
	got    string
	useErr error
	used   string
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

func (s *cacheStore) CacheObjectEnsureExists(cacheName, key string) error {
	if s.getErr == nil {
		s.got = key
	}
	return s.getErr
}

func (s *cacheStore) CacheObjectMarkUse(cacheName, key string, accessTime time.Time) error {
	if s.useErr == nil {
		s.used = key
	}
	return s.useErr
}

func (s *MetadataServerSuite) TestNew(c *check.C) {
	parentServer := &DummyStorageServer{}
	cstore := &cacheStore{}
	server := NewMetadataStorageServer(MetadataStorageServerArgs{
		Name:   "test",
		Server: parentServer,
		Store:  cstore,
	})
	c.Check(server, check.DeepEquals, &MetadataStorageServer{
		StorageServer: parentServer,
		name:          "test",
		store:         cstore,
	})
}

func (s *MetadataServerSuite) TestGetServerErr(c *check.C) {
	parentServer := &DummyStorageServer{
		GetErr: errors.New("get error"),
	}
	cstore := &cacheStore{}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	_, _, _, _, ok, err := server.Get("testdir", "storageaddress")
	c.Check(ok, check.Equals, false)
	c.Check(err, check.ErrorMatches, "get error")
}

func (s *MetadataServerSuite) TestGetStoreErr(c *check.C) {
	parentServer := &DummyStorageServer{
		GetOk: true,
	}
	cstore := &cacheStore{
		useErr: errors.New("store get error"),
	}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	_, _, _, _, ok, err := server.Get("testdir", "storageaddress")
	c.Check(ok, check.Equals, false)
	c.Check(err, check.ErrorMatches, "store get error")
}

func (s *MetadataServerSuite) TestGetOk(c *check.C) {
	f := &FakeFileIOFile{}
	parentServer := &DummyStorageServer{
		GetOk:     true,
		GetReader: f,
	}
	cstore := &cacheStore{}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	r, _, _, _, ok, err := server.Get("somedir", "storageaddress")
	c.Check(r, check.DeepEquals, f)
	c.Check(ok, check.Equals, true)
	c.Check(err, check.IsNil)
	c.Check(cstore.used, check.Equals, "somedir/storageaddress")
}

func (s *MetadataServerSuite) TestPutServerErr(c *check.C) {
	parentServer := &DummyStorageServer{
		PutErr: errors.New("put error"),
	}
	cstore := &cacheStore{}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "adir", "storageaddress")
	c.Check(err, check.ErrorMatches, "put error")
}

func (s *MetadataServerSuite) TestPutStoreErr(c *check.C) {
	parentServer := &DummyStorageServer{}
	cstore := &cacheStore{
		getErr: errors.New("use error"),
	}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "", "", nil
	}
	_, _, err := server.Put(resolve, "adir", "storageaddress")
	c.Check(err, check.ErrorMatches, "use error")
}

func (s *MetadataServerSuite) TestPutOk(c *check.C) {
	parentServer := &DummyStorageServer{}
	cstore := &cacheStore{}
	server := &MetadataStorageServer{
		StorageServer: parentServer,
		store:         cstore,
		name:          "test",
	}
	resolve := func(w io.Writer) (string, string, error) {
		return "adir", "storageaddress", nil
	}
	_, _, err := server.Put(resolve, "", "")
	c.Check(err, check.IsNil)
	c.Check(cstore.got, check.Equals, "adir/storageaddress")
}
