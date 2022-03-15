package postgrespq

// Copyright (C) 2022 by RStudio, PBC.

import (
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	lgr := &listener.TestLogger{}
	fakeFactory := &testFactory{}
	l3 := NewListenerFactory(ListenerFactoryArgs{
		Factory:     fakeFactory,
		DebugLogger: lgr,
	})
	c.Check(l3, check.DeepEquals, &ListenerFactory{
		factory:     fakeFactory,
		debugLogger: lgr,
	})
}
