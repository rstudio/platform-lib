package storage

// Copyright (C) 2022 by Posit Software, PBC

import (
	"errors"
	"testing"
	"time"

	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/notifytypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	storagetypes "github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type fakeBroadcaster struct {
	l chan listener.Notification
}

func (f *fakeBroadcaster) IP() string {
	return ""
}

func (f *fakeBroadcaster) Subscribe(dataType uint8) <-chan listener.Notification {
	return f.l
}

func (f *fakeBroadcaster) SubscribeOne(dataType uint8, matcher broadcaster.Matcher) <-chan listener.Notification {
	return f.l
}

func (f *fakeBroadcaster) Unsubscribe(ch <-chan listener.Notification) {
}

type dummyStore struct {
	notify error
}

func (d *dummyStore) Notify(channel string, n interface{}) error {
	return d.notify
}

type ChunksSuite struct {
}

var _ = check.Suite(&ChunksSuite{})

func (s *ChunksSuite) TestWait(c *check.C) {
	awb := &fakeBroadcaster{
		l: make(chan listener.Notification),
	}
	w := NewExampleChunkWaiter(awb)
	c.Assert(w, check.DeepEquals, &ExampleChunkWaiter{awb: awb})

	// Test waiting for chunk (ok)
	cn := &storagetypes.ChunkNotification{
		Timeout: time.Minute,
		Chunk:   3,
	}
	go func() {
		awb.l <- notifytypes.NewChunkNotification("", 4)
	}()
	w.WaitForChunk(cn)

	// Test waiting for chunk (timeout)
	cn = &storagetypes.ChunkNotification{
		Timeout: time.Millisecond,
		Chunk:   3,
	}
	w.WaitForChunk(cn)
}

func (s *ChunksSuite) TestNotify(c *check.C) {
	cstore := &dummyStore{
		notify: errors.New("some error"),
	}
	cn := NewExampleChunkNotifier(cstore)
	c.Assert(cn, check.DeepEquals, &ExampleChunkNotifier{store: cstore})

	err := cn.Notify(&storagetypes.ChunkNotification{})
	c.Assert(err, check.ErrorMatches, "some error")

	cstore.notify = nil
	err = cn.Notify(&storagetypes.ChunkNotification{})
	c.Assert(err, check.IsNil)
}

func (s *ChunksSuite) TestMatch(c *check.C) {
	cm := &ExampleChunkMatcher{}
	n := notifytypes.NewChunkNotification("myAddress", 10)
	c.Assert(cm.Match(n, "myAddress"), check.Equals, true)
	c.Assert(cm.Match(n, "notMyAddress"), check.Equals, false)
}
