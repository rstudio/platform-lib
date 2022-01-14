package local

// Copyright (C) 2022 by RStudio, PBC.

import (
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerfactory"
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
	llf := NewListenerProvider()
	cstore := &fakeStore{
		llf: llf,
	}
	l := NewListenerFactory(cstore.GetLocalListenerFactory())
	c.Check(l, check.DeepEquals, &ListenerFactory{
		llf: llf,
		CommonListenerFactory: listenerfactory.CommonListenerFactory{
			Unmarshallers: make(map[uint8]listener.Unmarshaller),
		},
	})
}
