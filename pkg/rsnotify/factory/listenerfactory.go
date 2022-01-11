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
	"github.com/rstudio/platform-lib/pkg/rsnotify/pglistener"
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

type PostgresListenerFactory struct {
	commonListenerFactory
	pool        *pgxpool.Pool
	debugLogger listener.DebugLogger
	listeners   []*pglistener.PostgresListener
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

func NewPostgresListenerFactory(pool *pgxpool.Pool, debugLogger listener.DebugLogger) *PostgresListenerFactory {
	return &PostgresListenerFactory{
		pool:        pool,
		debugLogger: debugLogger,
		commonListenerFactory: commonListenerFactory{
			unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	}
}

func (l *PostgresListenerFactory) Shutdown() {
	for _, listener := range l.listeners {
		listener.Stop()
	}
}

func (l *PostgresListenerFactory) New(channelName string, dataType listener.Notification) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	listener := pglistener.NewPostgresListener(channelName, dataType, l.pool, l.unmarshallers, l.debugLogger)
	l.listeners = append(l.listeners, listener)
	return listener
}

func (l *commonListenerFactory) RegisterUnmarshaler(dataType uint8, unmarshaler listener.Unmarshaller) {
	if _, ok := l.unmarshallers[dataType]; ok {
		log.Fatalf("Attempted to register a listener unmarshaler for a type (%d) that is already registered", dataType)
	}
	l.unmarshallers[dataType] = unmarshaler
}
