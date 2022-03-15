package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

type ListenerFactory struct {
	pool        *pgxpool.Pool
	debugLogger listener.DebugLogger
	listeners   []*PgxListener
	ipReporter  listener.IPReporter
}

type ListenerFactoryArgs struct {
	Pool        *pgxpool.Pool
	DebugLogger listener.DebugLogger
	IpReporter  listener.IPReporter
}

func NewListenerFactory(args ListenerFactoryArgs) *ListenerFactory {
	return &ListenerFactory{
		pool:        args.Pool,
		debugLogger: args.DebugLogger,
		ipReporter:  args.IpReporter,
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
		Name:        channelName,
		Pool:        l.pool,
		Matcher:     matcher,
		DebugLogger: l.debugLogger,
		IpReporter:  l.ipReporter,
	})
	l.listeners = append(l.listeners, pgxListener)
	return pgxListener
}
