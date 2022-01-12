package listenerfactory

/* listenerfactory.go
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

import (
	"log"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
	"github.com/rstudio/platform-lib/pkg/rsnotify/locallistener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/pgxlistener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/pqlistener"
)

type ListenerFactory interface {
	New(channel string, dataType listener.Notification) listener.Listener
	RegisterUnmarshaler(dataType uint8, unmarshaler listener.Unmarshaller)
	Shutdown()
}

type commonListenerFactory struct {
	// A map of registered unmarshallers for the message types we support
	unmarshallers map[uint8]listener.Unmarshaller
}

type LocalListenerFactory struct {
	commonListenerFactory
	llf *locallistener.LocalListenerFactory
}

type PgxListenerFactory struct {
	commonListenerFactory
	pool        *pgxpool.Pool
	debugLogger listener.DebugLogger
	listeners   []*pgxlistener.PgxListener
}

type PqListenerFactory struct {
	commonListenerFactory
	factory     pqlistener.PqRetrieveListenerFactory
	debugLogger listener.DebugLogger
	listeners   []*pqlistener.PqListener
}

func NewLocalListenerFactory(llf *locallistener.LocalListenerFactory) *LocalListenerFactory {
	return &LocalListenerFactory{
		llf: llf,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *LocalListenerFactory) Shutdown() {}

func (l *LocalListenerFactory) New(channelName string, dataType listener.Notification) listener.Listener {
	return l.llf.New(channelName)
}

func NewPgxListenerFactory(pool *pgxpool.Pool, debugLogger listener.DebugLogger) *PgxListenerFactory {
	return &PgxListenerFactory{
		pool:        pool,
		debugLogger: debugLogger,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *PgxListenerFactory) Shutdown() {
	for _, listener := range l.listeners {
		listener.Stop()
	}
}

func (l *PgxListenerFactory) New(channelName string, dataType listener.Notification) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	listener := pgxlistener.NewPgxListener(channelName, dataType, l.pool, l.unmarshallers, l.debugLogger)
	l.listeners = append(l.listeners, listener)
	return listener
}

func NewPqListenerFactory(factory pqlistener.PqRetrieveListenerFactory, debugLogger listener.DebugLogger) *PqListenerFactory {
	return &PqListenerFactory{
		factory:     factory,
		debugLogger: debugLogger,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *PqListenerFactory) Shutdown() {
	for _, listener := range l.listeners {
		listener.Stop()
	}
}

func (l *PqListenerFactory) New(channelName string, dataType listener.Notification) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	listener := pqlistener.NewPqListener(channelName, dataType, l.factory, l.unmarshallers, l.debugLogger)
	l.listeners = append(l.listeners, listener)
	return listener
}

func (l *commonListenerFactory) RegisterUnmarshaler(dataType uint8, unmarshaler listener.Unmarshaller) {
	if _, ok := l.unmarshallers[dataType]; ok {
		log.Fatalf("Attempted to register a listener unmarshaler for a type (%d) that is already registered", dataType)
	}
	l.unmarshallers[dataType] = unmarshaler
}
