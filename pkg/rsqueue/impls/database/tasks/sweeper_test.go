package tasks

// Copyright (C) 2025 by Posit Software, PBC

import (
	"context"
	"errors"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database/dbqueuetypes"
	"gopkg.in/check.v1"
)

type SweeperSuite struct {
	store *QueueTestStore
}

var _ = check.Suite(&SweeperSuite{})

func (s *SweeperSuite) SetUpSuite(c *check.C) {
	s.store = &QueueTestStore{}
}

func (s *SweeperSuite) SetUpTest(c *check.C) {
	s.store.err = nil
	s.store.hasAddress = true
}

func (s *SweeperSuite) TestSweepOk(c *check.C) {
	cstore := &QueueTestStore{
		permits: []dbqueuetypes.QueuePermit{
			&fakePermit{
				permitId: 23,
			},
			&fakePermit{
				permitId: 24,
			},
			&fakePermit{
				permitId: 25,
			},
		},
	}
	monitor := &fakeMonitor{
		resultMap: map[uint64]bool{
			23: true,
		},
	}
	q := &DatabaseQueueSweeperTask{
		store:   cstore,
		monitor: monitor,
	}

	q.Run(context.Background())
	c.Assert(cstore.permitsCalled, check.Equals, 1)
	c.Assert(cstore.permitsDeleted, check.Equals, 2)
}

func (s *SweeperSuite) TestSweepErrs(c *check.C) {
	cstore := &QueueTestStore{
		permitsErr: errors.New("cannot list permits"),
	}
	q := &DatabaseQueueSweeperTask{
		store: cstore,
	}

	q.Run(context.Background())
	c.Assert(cstore.permitsCalled, check.Equals, 1)
}
