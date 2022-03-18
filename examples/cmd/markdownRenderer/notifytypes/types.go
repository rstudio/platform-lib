package notifytypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

// NotifyTypeQueue: Queue work is ready
// NotifyTypeWorkComplete: Addressed work is complete
// NotifyTypeChunk: Chunked download chunk is ready
// NotifyTypePermitExtend: Extend a queue work permit
const (
	NotifyTypeQueue        = uint8(1)
	NotifyTypeWorkComplete = uint8(2)
	NotifyTypeChunk        = uint8(3)
	NotifyTypePermitExtend = uint8(4)
)

// ChannelMessages: used by for notifications for all nodes (e.g., queue, cache)
// ChannelLeader: used for messages designated for the elected leader node.
// ChannelFollower: used for messages designated for non-leader nodes.
const (
	ChannelMessages = "messages"
	ChannelLeader   = "leader"
	ChannelFollower = "follower"
)

// ChunkNotification is a notification type sent when new chunks are available
// for an asset stored in chunks.
type ChunkNotification struct {
	listener.GenericNotification
	Address string
	Chunk   uint64
}

func NewChunkNotification(address string, chunk uint64) *ChunkNotification {
	return &ChunkNotification{
		GenericNotification: listener.GenericNotification{
			NotifyGuid: uuid.New().String(),
			NotifyType: NotifyTypeChunk,
		},
		Address: address,
		Chunk:   chunk,
	}
}
