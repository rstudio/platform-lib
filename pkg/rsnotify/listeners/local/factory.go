package local

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerfactory"
)

type ListenerFactory struct {
	listenerfactory.CommonListenerFactory
	llf *ListenerProvider
}

func NewListenerFactory(llf *ListenerProvider) *ListenerFactory {
	return &ListenerFactory{
		llf: llf,
		CommonListenerFactory: listenerfactory.CommonListenerFactory{
			Unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *ListenerFactory) Shutdown() {}

func (l *ListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	return l.llf.New(channelName)
}
