package listenerfactory

// Copyright (C) 2022 by RStudio, PBC.

import (
	"log"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type ListenerFactory interface {
	New(channel string, matcher listener.TypeMatcher) listener.Listener
	RegisterUnmarshaler(dataType uint8, unmarshaler listener.Unmarshaller)
	Shutdown()
}

type CommonListenerFactory struct {
	// A map of registered unmarshallers for the message types we support
	Unmarshallers map[uint8]listener.Unmarshaller
}

func (l *CommonListenerFactory) RegisterUnmarshaler(dataType uint8, unmarshaler listener.Unmarshaller) {
	if _, ok := l.Unmarshallers[dataType]; ok {
		log.Fatalf("Attempted to register a listener unmarshaler for a type (%d) that is already registered", dataType)
	}
	l.Unmarshallers[dataType] = unmarshaler
}
