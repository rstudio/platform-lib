package localelection

// Copyright (C) 2022 by Posit Software, PBC

import (
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/rstudio/platform-lib/v2/pkg/rselection"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type LocalLeaderSuite struct{}

var _ = check.Suite(&LocalLeaderSuite{})

type fakeTaskHandler struct {
	verify chan chan bool
	mutex  sync.RWMutex
}

func (f *fakeTaskHandler) Handle(b broadcaster.Broadcaster) {}

func (f *fakeTaskHandler) Register(name string, task rselection.Task) {}

func (f *fakeTaskHandler) Stop() {}

func (f *fakeTaskHandler) Verify() <-chan chan bool {
	return f.verify
}

func (s *LocalLeaderSuite) TestLead(c *check.C) {
	defer leaktest.Check(c)

	lf := local.NewListenerProvider(local.ListenerProviderArgs{})
	l := lf.New(c.TestName())
	defer l.Stop()
	awbStop := make(chan bool)
	awb, err := broadcaster.NewNotificationBroadcaster(l, awbStop)
	c.Assert(err, check.IsNil)
	defer func() {
		awbStop <- true
	}()

	verify := make(chan chan bool)
	taskHandler := &fakeTaskHandler{
		verify: verify,
	}
	stop := make(chan bool)
	sl := NewLocalLeader(LocalLeaderConfig{
		Broadcaster: awb,
		TaskHandler: taskHandler,
		StopChannel: stop,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := sl.Lead()
		c.Assert(err, check.IsNil)
	}()

	// Wait for verify channel to be ready
	for {
		if func() bool {
			taskHandler.mutex.RLock()
			defer taskHandler.mutex.RUnlock()
			if taskHandler.verify != nil {
				return true
			}
			return false
		}() {
			break
		}
	}

	// Verification always returns true
	vCh := make(chan bool)
	taskHandler.verify <- vCh
	result := <-vCh
	c.Assert(result, check.Equals, true)

	close(stop)
	<-done
}
