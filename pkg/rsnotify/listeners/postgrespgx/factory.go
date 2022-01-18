package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerfactory"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

type PgxListenerFactory struct {
	listenerfactory.CommonListenerFactory
	pool        *pgxpool.Pool
	debugLogger listener.DebugLogger
	listeners   []*PgxListener
}

func NewPgxListenerFactory(pool *pgxpool.Pool, debugLogger listener.DebugLogger) *PgxListenerFactory {
	return &PgxListenerFactory{
		pool:        pool,
		debugLogger: debugLogger,
		CommonListenerFactory: listenerfactory.CommonListenerFactory{
			Unmarshallers: make(map[uint8]listener.Unmarshaller),
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

	listener := NewPgxListener(channelName, dataType, l.pool, l.Unmarshallers, l.debugLogger)
	l.listeners = append(l.listeners, listener)
	return listener
}
