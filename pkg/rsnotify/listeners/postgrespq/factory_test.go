package postgrespq

// Copyright (C) 2022 by Posit, PBC.

import (
	"gopkg.in/check.v1"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	fakeFactory := &testFactory{}
	l3 := NewListenerFactory(ListenerFactoryArgs{
		Factory: fakeFactory,
	})
	c.Check(l3, check.DeepEquals, &ListenerFactory{
		factory: fakeFactory,
	})
}
