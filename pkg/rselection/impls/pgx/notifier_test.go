package pgxelection

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rstudio/platform-lib/pkg/rselection/electiontypes"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx"
	"gopkg.in/check.v1"
)

type NotifierSuite struct {
	pool *pgxpool.Pool
}

var _ = check.Suite(&NotifierSuite{})

func (s *NotifierSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("NotifierSuite only runs on Postgres")
	}

	var err error
	s.pool, err = EphemeralPgxPool("postgres")
	c.Assert(err, check.IsNil)
}

func (s *NotifierSuite) TearDownSuite(c *check.C) {
	if testing.Short() {
		c.Skip("NotifierSuite only runs on Postgres")
	}

	s.pool.Close()
}

func (s *NotifierSuite) TestSafeChannelName(c *check.C) {
	c.Assert(safeChannelName("channel-name"), check.Equals, "channel-name")
	c.Assert(safeChannelName("unsafe-and-very-long-channel-name-unsafe-and-very-long-channel-name"),
		check.Equals, "711f3d7bab0f98a92ea06fc0e89ad479")
}

func (s *NotifierSuite) TestNotify(c *check.C) {
	channel := c.TestName()
	notifier := &PgxPgNotifier{
		pool: s.pool,
	}
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	plf := postgrespgx.NewPgxListener(channel, s.pool, matcher, nil)
	defer plf.Stop()

	ping1 := &electiontypes.ClusterPingRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			MessageType: electiontypes.ClusterMessageTypePing,
			GuidVal:     "123",
		},
	}
	ping2 := &electiontypes.ClusterPingRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			MessageType: electiontypes.ClusterMessageTypePing,
			GuidVal:     "456",
		},
	}

	items, errs, err := plf.Listen()
	c.Assert(err, check.IsNil)
	go func() {
		b1, err := json.Marshal(ping1)
		c.Assert(err, check.IsNil)
		b2, err := json.Marshal(ping2)
		c.Assert(err, check.IsNil)
		notifier.Notify("wrong-channel", b1)
		notifier.Notify(channel, b2)
	}()

	select {
	case err := <-errs:
		c.Fatalf("error received: %s", err)
	case n := <-items:
		c.Assert(n, check.DeepEquals, ping2)
	}
}
