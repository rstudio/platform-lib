package pgxelection_test

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rstudio/platform-lib/v4/pkg/rselection"
	"github.com/rstudio/platform-lib/v4/pkg/rselection/electiontypes"
	pgxelection "github.com/rstudio/platform-lib/v4/pkg/rselection/impls/pgx"
	"github.com/rstudio/platform-lib/v4/pkg/rsnotify/broadcaster"
)

// nodeStore lists the nodes that the database considers part of the cluster.
type nodeStore struct{}

func (nodeStore) Nodes() (map[string]*electiontypes.ClusterNode, error) {
	return map[string]*electiontypes.ClusterNode{}, nil
}

// assumeLeaderQueue receives the request that a follower makes when it hears
// no pings from a leader.
type assumeLeaderQueue struct{}

func (assumeLeaderQueue) Push(ctx context.Context, w electiontypes.AssumeLeader) error {
	return nil
}

// This example is compiled but not run, because it needs a PostgreSQL
// database. Each node follows until it is promoted, leads until it stops
// leading, and then follows again.
func ExamplePgxLeader() {
	var (
		pool *pgxpool.Pool           // A connection pool for the shared database.
		awb  broadcaster.Broadcaster // A broadcaster that listens on the leader, follower and messages channels.
	)
	ctx := context.Background()
	notifier := pgxelection.NewPgxPgNotifier(pgxelection.PgxPgNotifierConfig{Pool: pool})
	handler := rselection.NewGenericTaskHandler(rselection.GenericTaskHandlerConfig{})

	// The follower and the leader share this channel. Close it when the
	// service shuts down. That stops whichever one is running and ends the
	// loop.
	stop := make(chan bool)

	for {
		follower := pgxelection.NewPgxFollower(pgxelection.PgxFollowerConfig{
			Queue:         assumeLeaderQueue{},
			Broadcaster:   awb,
			Notifier:      notifier,
			LeaderChannel: "election-leader",
			Address:       "node-1",
			StopChan:      stop,
			Timeout:       10 * time.Second,
		})

		// Follow blocks until the node is told to stop or is promoted.
		if follower.Follow(ctx) != pgxelection.FollowResultPromote {
			return
		}

		leader := pgxelection.NewPgxLeader(pgxelection.PgxLeaderConfig{
			Store:           nodeStore{},
			Broadcaster:     awb,
			Notifier:        notifier,
			TaskHandler:     handler,
			LeaderChannel:   "election-leader",
			FollowerChannel: "election-follower",
			MessagesChannel: "election-messages",
			Address:         "node-1",
			StopChan:        stop,
			PingInterval:    5 * time.Second,
			SweepInterval:   10 * time.Second,
			MaxPingAge:      30 * time.Second,
			StepDownTimeout: time.Minute,
		})

		// Lead blocks until the leader stops, for example because it heard
		// from another leader. The loop then goes back to following.
		if err := leader.Lead(ctx); err != nil {
			return
		}
	}
}
