package listener

// Copyright (C) 2022 by RStudio, PBC.

import "encoding/json"

const (
	// MaxChannelSize sizes for the channel to prevent blocking when distributing
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

type Unmarshaller func(n Notification, rawMap map[string]*json.RawMessage) error

type TypeMatcher interface {
	Field() string
	Register(notifyType uint8, dataType interface{})
	Type(notifyType uint8) interface{}
}

type Notification interface {
	Type() uint8
	Guid() string
	Data() interface{}
}

type Listener interface {
	Listen() (items chan Notification, errs chan error, err error)
	Stop()
	IP() string
}

type Logger interface {
	Debugf(msg string, argslistenerfactory ...interface{})
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

type GenericMatcher struct {
	field string
	types map[uint8]interface{}
}

func (m *GenericMatcher) Field() string {
	return m.field
}

func (m *GenericMatcher) Type(notifyType uint8) interface{} {
	return m.types[notifyType]
}

func (m *GenericMatcher) Register(notifyType uint8, dataType interface{}) {
	m.types[notifyType] = dataType
}

func NewMatcher(field string) *GenericMatcher {
	return &GenericMatcher{
		field: field,
		types: make(map[uint8]interface{}),
	}
}
