package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/jackc/pgx/v4/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	pool := &pgxpool.Pool{}
	lgr := &listener.TestLogger{}
	l2 := NewPgxListenerFactory(pool, lgr)
	c.Check(l2, check.DeepEquals, &PgxListenerFactory{
		pool:        pool,
		debugLogger: lgr,
	})
}
