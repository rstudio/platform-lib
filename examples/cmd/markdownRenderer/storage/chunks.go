package storage

// Copyright (C) 2022 by Posit Software, PBC

import (
	"time"

	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/notifytypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/types"
)

type ExampleChunkWaiter struct {
	awb broadcaster.Broadcaster
}

func (cw *ExampleChunkWaiter) WaitForChunk(c *types.ChunkNotification) {
	timeout := time.NewTimer(c.Timeout)
	defer timeout.Stop()

	chunkListener := cw.awb.SubscribeOne(notifytypes.NotifyTypeChunk, func(n listener.Notification) bool {
		if cn, ok := n.(*notifytypes.ChunkNotification); ok {
			return cn.Address == c.Address && cn.Chunk >= c.Chunk
		}
		return false
	})
	defer cw.awb.Unsubscribe(chunkListener)

	// Wait for a timeout or a chunk notification that matches the address
	func() {
		for {
			select {
			case <-chunkListener:
				return
			case <-timeout.C:
				return
			}
		}
	}()

}

func NewExampleChunkWaiter(awb broadcaster.Broadcaster) *ExampleChunkWaiter {
	return &ExampleChunkWaiter{awb: awb}
}

type ChunkNotifierStore interface {
	Notify(channelName string, n interface{}) error
}

type ExampleChunkNotifier struct {
	store ChunkNotifierStore
}

func (cn *ExampleChunkNotifier) Notify(c *types.ChunkNotification) error {
	return cn.store.Notify(notifytypes.ChannelMessages, notifytypes.NewChunkNotification(c.Address, c.Chunk))
}

func NewExampleChunkNotifier(cstore ChunkNotifierStore) *ExampleChunkNotifier {
	return &ExampleChunkNotifier{store: cstore}
}

type ExampleChunkMatcher struct{}

func (cn *ExampleChunkMatcher) Match(n listener.Notification, address string) bool {
	if cn, ok := n.(*notifytypes.ChunkNotification); ok {
		return cn.Address == address
	}
	return false
}
