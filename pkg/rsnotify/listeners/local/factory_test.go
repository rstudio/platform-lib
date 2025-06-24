package local

// Copyright (C) 2022 by Posit, PBC.

import (
	"gopkg.in/check.v1"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

type fakeStore struct {
	llf *ListenerProvider
}

func (f *fakeStore) GetLocalListenerFactory() *ListenerProvider {
	return f.llf
}

func (s *ListenerFactorySuite) TestNewListener(c *check.C) {
	llf := NewListenerProvider(ListenerProviderArgs{})
	cstore := &fakeStore{
		llf: llf,
	}
	l := NewListenerFactory(cstore.GetLocalListenerFactory())
	c.Check(l, check.DeepEquals, &ListenerFactory{
		llf: llf,
	})
}
