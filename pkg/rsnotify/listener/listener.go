package listener

/* listener.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

const (
	// Sizing for the channel to prevent blocking when distributing
	// notifications to listeners.
	MaxChannelSize = 100
)

// NotifyTypeQueue: Queue work is ready
// NotifyTypeTx: Latest macro transaction has changed
// NotifyTypeWorkComplete: Addressed work is complete
// NotifyTypeSwitchMode: Service mode (online/offline state) switch
// NotifyTypeChunk: Chunked download chunk is ready
// NotifyDistroRefresh: Inform the cluster to perform a distro refresh simultaneously
const (
	NotifyTypeQueue        = uint8(1)
	NotifyTypeTx           = uint8(2)
	NotifyTypeWorkComplete = uint8(3)
	NotifyTypeSwitchMode   = uint8(4)
	NotifyTypeChunk        = uint8(5)
	NotifyDistroRefresh    = uint8(6)
)

const (
	ChannelMessages = "messages"
	ChannelLeader   = "leader"
	ChannelFollower = "follower"
)

type Unmarshaller func(n Notification, data []byte) error

type Notification interface {
	Type() uint8
	Guid() string
	Data() interface{}
}

type Listener interface {
	Listen() (items chan Notification, errs chan error, err error)
	Stop()
}

type Logger interface {
	Debugf(msg string, args ...interface{})
}

type DebugLogger interface {
	Logger
	Enabled() bool
}

type GenericNotification struct {
	NotifyGuid string
	NotifyData interface{}
	NotifyType uint8
}

func (n *GenericNotification) Type() uint8 {
	return n.NotifyType
}

func (n *GenericNotification) Guid() string {
	return n.NotifyGuid
}

func (n *GenericNotification) Data() interface{} {
	return n.NotifyData
}
