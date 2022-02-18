package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

type PgxListenerFactory struct {
	pool        *pgxpool.Pool
	debugLogger listener.DebugLogger
	listeners   []*PgxListener
}

func NewPgxListenerFactory(pool *pgxpool.Pool, debugLogger listener.DebugLogger) *PgxListenerFactory {
	return &PgxListenerFactory{
		pool:        pool,
		debugLogger: debugLogger,
	}
}

func (l *PgxListenerFactory) Shutdown() {
	for _, ll := range l.listeners {
		ll.Stop()
	}
}

func (l *PgxListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	pgxListener := NewPgxListener(channelName, l.pool, matcher, l.debugLogger)
	l.listeners = append(l.listeners, pgxListener)
	return pgxListener
}
