package postgrespq

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerfactory"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

type PqListenerFactory struct {
	listenerfactory.CommonListenerFactory
	factory     PqRetrieveListenerFactory
	debugLogger listener.DebugLogger
	listeners   []*PqListener
}

func NewPqListenerFactory(factory PqRetrieveListenerFactory, debugLogger listener.DebugLogger) *PqListenerFactory {
	return &PqListenerFactory{
		factory:     factory,
		debugLogger: debugLogger,
		CommonListenerFactory: listenerfactory.CommonListenerFactory{
			Unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *PqListenerFactory) Shutdown() {
	for _, listener := range l.listeners {
		listener.Stop()
	}
}

func (l *PqListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	listener := NewPqListener(channelName, l.factory, matcher, l.Unmarshallers, l.debugLogger)
	l.listeners = append(l.listeners, listener)
	return listener
}
