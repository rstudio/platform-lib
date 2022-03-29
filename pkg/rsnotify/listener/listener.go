package listener

// Copyright (C) 2022 by RStudio, PBC.

const (
	// MaxChannelSize sizes the channel to prevent blocking when distributing
	// notifications to listeners.
	MaxChannelSize = 100
)

type TypeMatcher interface {
	Field() string
	Register(notifyType uint8, dataType interface{})
	Type(notifyType uint8) (interface{}, error)
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
	Debugf(msg string, args ...interface{})
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
