package tasks

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"errors"

	"github.com/rstudio/platform-lib/v4/pkg/rsqueue/queue"
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
		permits: []queue.QueuePermit{
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

// TestSweepOkCommits establishes the baseline for the test below: a sweep that
// deletes every expired permit without error completes with a nil error, which is
// what a real store commits on.
func (s *SweeperSuite) TestSweepOkCommits(c *check.C) {
	cstore := &QueueTestStore{
		permits: []queue.QueuePermit{
			&fakePermit{permitId: 23},
			&fakePermit{permitId: 24},
		},
	}
	q := &DatabaseQueueSweeperTask{
		store:   cstore,
		monitor: &fakeMonitor{},
	}

	q.Run(context.Background())
	c.Assert(cstore.permitsDeleted, check.Equals, 2)
	c.Assert(cstore.completeCalled, check.Equals, 1)
	c.Assert(cstore.completedWith, check.IsNil)
}

// TestSweepDeleteErrRollsBack covers a failure part-way through the sweep.
//
// Run deletes each expired permit inside one transaction, and the deferred
// CompleteTransaction decides between COMMIT and ROLLBACK by reading the `err`
// variable. A `:=` on the delete inside the loop redeclared that variable, so a
// mid-loop failure returned with the outer error still nil and the transaction
// committed: the permits deleted before the failure were made permanent, the rest
// were abandoned, and the caller was told the sweep succeeded.
//
// Asserting on completedWith rather than on the delete count is the point: the
// buggy and fixed versions attempt exactly the same deletes and both return at the
// first failure, so no count can tell them apart. The only observable difference is
// the error the transaction is completed with, which is what decides whether the
// partial result is kept.
func (s *SweeperSuite) TestSweepDeleteErrRollsBack(c *check.C) {
	deleteErr := errors.New("cannot delete permit")
	cstore := &QueueTestStore{
		permits: []queue.QueuePermit{
			&fakePermit{permitId: 23},
			&fakePermit{permitId: 24},
		},
		permitDelete: deleteErr,
	}
	q := &DatabaseQueueSweeperTask{
		store:   cstore,
		monitor: &fakeMonitor{},
	}

	q.Run(context.Background())

	c.Assert(cstore.completeCalled, check.Equals, 1)
	c.Assert(cstore.completedWith, check.Equals, deleteErr,
		check.Commentf("a failed permit delete must roll the sweep back, not commit it"))
}
