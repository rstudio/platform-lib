package rscache

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log/slog"
	"time"
)

const LevelTrace = slog.Level(-8)

type ResolverSpec struct {
	Priority      uint64
	GroupId       int64
	CacheInMemory bool
	Retries       int
	Work          AddressableWork

	// If true, cached GOBs are gzipped
	Gzip bool

	// If true, don't return a reader. Assume this is for a HEAD request
	// and return only the size and modification time
	Head bool
}

type AddressableWork interface {
	Address() string
	Dir() string
	Type() uint64
}

func (r ResolverSpec) Address() string {
	return r.Work.Address()
}

func (r ResolverSpec) Dir() string {
	return r.Work.Dir()
}

type CacheReturn struct {
	Value        interface{}
	ReturnedFrom string
	CacheKey     string
	Complete     bool
	Err          error
	Size         int64
	Timestamp    time.Time
}

func (r CacheReturn) AsReader() (reader io.ReadCloser, err error) {
	if r.Err != nil {
		return nil, r.Err
	}

	// try to cast as bytes. If this fails, it means it's an object and GetObject is what should be used.
	if r.Value == nil {
		return nil, fmt.Errorf("attempted to retrieve nil value from CacheReturn; specified key was %s", r.CacheKey)
	} else if castBytes, ok := r.Value.([]byte); ok {
		reader = ioutil.NopCloser(bytes.NewReader(castBytes))
	} else if reader, ok = r.Value.(io.ReadCloser); ok {
		return
	} else {
		err = fmt.Errorf("failed to get io.ReadCloser for cached bytes from reader; specified key was %s", r.CacheKey)
	}
	return
}

func (r CacheReturn) AsObject() (interface{}, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	if r.Value == nil {
		return nil, fmt.Errorf("attempted to retrieve nil value from CacheReturn; specified key was %s", r.CacheKey)
	}
	return r.Value, nil
}

func (r CacheReturn) GetSize() int64 {
	return r.Size
}

func (r CacheReturn) GetTimestamp() time.Time {
	return r.Timestamp
}

// IsNull returns `true` if this is an empty result (i.e. element not found)
func (r CacheReturn) IsNull() bool {
	return r.Value == nil
}

// IsComplete returns `true` if we have the entire asset cached.
func (r CacheReturn) IsComplete() bool {
	return r.Complete
}

func (r CacheReturn) Error() error {
	return r.Err
}

type CacheProvider interface {
	// Get - the CacheReturn here ultimately ends up returning an io.ReadCloser.
	Get(ctx context.Context, resolver ResolverSpec) (value CacheReturn)
	// GetObject - for objects that will be decoded from gob files, we must pass in the type example.
	GetObject(ctx context.Context, resolver ResolverSpec, typeExample interface{}) (value CacheReturn)
	Check(resolver ResolverSpec) (bool, error)
	Head(ctx context.Context, resolver ResolverSpec) (size int64, modTime time.Time, err error)
	Uncache(resolver ResolverSpec) error
}
