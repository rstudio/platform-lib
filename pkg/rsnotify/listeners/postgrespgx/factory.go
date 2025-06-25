package postgrespgx

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listenerutils"
)

type ListenerFactory struct {
	pool       *pgxpool.Pool
	listeners  []*PgxListener
	ipReporter listener.IPReporter
}

type ListenerFactoryArgs struct {
	Pool       *pgxpool.Pool
	IpReporter listener.IPReporter
}

func NewListenerFactory(args ListenerFactoryArgs) *ListenerFactory {
	return &ListenerFactory{
		pool:       args.Pool,
		ipReporter: args.IpReporter,
	}
}

func (l *ListenerFactory) Shutdown() {
	for _, ll := range l.listeners {
		ll.Stop()
	}
}

func (l *ListenerFactory) New(channelName string, matcher listener.TypeMatcher) listener.Listener {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	pgxListener := NewPgxListener(PgxListenerArgs{
		Name:       channelName,
		Pool:       l.pool,
		Matcher:    matcher,
		IpReporter: l.ipReporter,
	})
	l.listeners = append(l.listeners, pgxListener)
	return pgxListener
}
