package postgrespq

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

type ListenerFactory struct {
	factory     PqRetrieveListenerFactory
	debugLogger listener.DebugLogger
	listeners   []*PqListener
}

type ListenerFactoryArgs struct {
	Factory     PqRetrieveListenerFactory
	DebugLogger listener.DebugLogger
}

func NewListenerFactory(args ListenerFactoryArgs) *ListenerFactory {
	return &ListenerFactory{
		factory:     args.Factory,
		debugLogger: args.DebugLogger,
	}
}

func (l *ListenerFactory) Shutdown() {
	for _, listener := range l.listeners {
		listener.Stop()
	}
}

func (l *ListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	listener := NewPqListener(PqListenerArgs{
		Name:        channelName,
		Factory:     l.factory,
		Matcher:     matcher,
		DebugLogger: l.debugLogger,
	})
	l.listeners = append(l.listeners, listener)
	return listener
}
