package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	pool := &pgxpool.Pool{}
	ipRep := &listener.TestIPReporter{}

	l2 := NewListenerFactory(ListenerFactoryArgs{Pool: pool, IpReporter: ipRep})
	c.Check(l2, check.DeepEquals, &ListenerFactory{
		pool:       pool,
		ipReporter: ipRep,
	})
}
