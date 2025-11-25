package local

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/listener"
)

type ListenerFactory struct {
	llf *ListenerProvider
}

func NewListenerFactory(llf *ListenerProvider) *ListenerFactory {
	return &ListenerFactory{
		llf: llf,
	}
}

func (l *ListenerFactory) Shutdown() {}

func (l *ListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	return l.llf.New(channelName)
}
