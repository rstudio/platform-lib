package listener

// Copyright (C) 2022 by RStudio, PBC.

const (
	// MaxChannelSize sizes for the channel to prevent blocking when distributing
	// notifications to listeners.
	MaxChannelSize = 100
)

type TypeMatcher interface {
	Field() string
	Register(notifyType uint8, dataType interface{})
	Type(notifyType uint8) interface{}
}

type Notification interface {
	Type() uint8
	Guid() string
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
	NotifyType uint8
}

func (n *GenericNotification) Type() uint8 {
	return n.NotifyType
}

func (n *GenericNotification) Guid() string {
	return n.NotifyGuid
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
