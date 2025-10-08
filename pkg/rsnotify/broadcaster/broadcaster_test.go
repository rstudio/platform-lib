package broadcaster

// Copyright (C) 2022 by RStudio, PBC.

import (
	"sync"
	"testing"

	"github.com/fortytw2/leaktest"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
)

type BroadcasterSuite struct{}

var _ = check.Suite(&BroadcasterSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

type FakeListener struct {
	items chan listener.Notification
	errs  chan error
	err   error
	ip    string
}

func (f *FakeListener) Listen() (items chan listener.Notification, errs chan error, err error) {
	return f.items, f.errs, f.err
}

func (f *FakeListener) Stop() {
	close(f.items)
	close(f.errs)
}

func (f *FakeListener) IP() string {
	return f.ip
}

func (s *BroadcasterSuite) TestNewNotificationBroadcaster(c *check.C) {
	items := make(chan listener.Notification)
	errs := make(chan error)
	l := &FakeListener{
		items: items,
		errs:  errs,
	}
	stop := make(chan bool)
	b, err := NewNotificationBroadcaster(l, stop)
	c.Check(err, check.IsNil)
	c.Check(b.listener, check.DeepEquals, l)
	c.Check(b.subscribe, check.NotNil)
	c.Check(b.unsubscribe, check.NotNil)
	c.Check(b.msgs, check.DeepEquals, items)
	c.Check(b.errs, check.DeepEquals, errs)
	c.Check(b.stopSignal, check.DeepEquals, stop)
}

func (s *BroadcasterSuite) TestBroadcast(c *check.C) {
	defer leaktest.Check(c)()

	items := make(chan listener.Notification)
	errs := make(chan error)
	l := &FakeListener{
		items: items,
		errs:  errs,
	}
	stop := make(chan bool)
	b, err := NewNotificationBroadcaster(l, stop)
	c.Check(err, check.IsNil)

	l1 := b.Subscribe(1)
	l2 := b.Subscribe(1)
	l3 := b.Subscribe(1)

	tn := &listener.TestNotification{
		Val:     "test",
		GuidVal: "someguid",
	}

	wg := &sync.WaitGroup{}
	wg.Add(3)
	var n1, n2, n3 *listener.TestNotification
	go func() {
		n := <-l1
		n1 = n.(*listener.TestNotification)
		wg.Done()
	}()
	go func() {
		n := <-l2
		n2 = n.(*listener.TestNotification)
		wg.Done()
	}()
	go func() {
		n := <-l3
		n3 = n.(*listener.TestNotification)
		wg.Done()
	}()

	// One notification
	items <- tn

	// Wait for all receipts
	wg.Wait()

	// The singular notification should have been received on all channels
	c.Assert(n1, check.DeepEquals, tn)
	c.Assert(n2, check.DeepEquals, tn)
	c.Assert(n3, check.DeepEquals, tn)

	// Unsubscribe
	b.Unsubscribe(l1)
	b.Unsubscribe(l2)
	b.Unsubscribe(l3)

	// Close listener
	l.Stop()
}

func (s *BroadcasterSuite) TestBroadcastOne(c *check.C) {
	defer leaktest.Check(c)()

	items := make(chan listener.Notification)
	errs := make(chan error)
	l := &FakeListener{
		items: items,
		errs:  errs,
	}
	stop := make(chan bool)
	b, err := NewNotificationBroadcaster(l, stop)
	c.Check(err, check.IsNil)

	l1 := b.SubscribeOne(1, func(n listener.Notification) bool {
		val := n.(*listener.TestNotification).Val == "test2"
		return val
	})

	tn := &listener.TestNotification{
		Val:     "test",
		GuidVal: "someguid",
	}
	tn2 := &listener.TestNotification{
		Val:     "test2",
		GuidVal: "someguid",
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	var n1 *listener.TestNotification
	go func() {
		n := <-l1
		n1 = n.(*listener.TestNotification)
		wg.Done()
	}()

	// One notification
	items <- tn

	// One targeted notification
	items <- tn2

	// Subsequent send ignored
	items <- tn2

	// Wait for all receipts
	wg.Wait()

	// The singular notification should have been received on all channels
	c.Assert(n1, check.DeepEquals, tn2)

	// Close listener
	l.Stop()
}

// Validate that a broadcaster is stopped (closing its output channels) when its input channel closes.
func (s *BroadcasterSuite) TestBroadcasterStop(c *check.C) {
	defer leaktest.Check(c)()

	items := make(chan listener.Notification)
	errs := make(chan error)
	l := &FakeListener{
		items: items,
		errs:  errs,
	}
	tn := &listener.TestNotification{
		Val:     "value",
		GuidVal: "myguid",
	}
	stop := make(chan bool)
	b, err := NewNotificationBroadcaster(l, stop)
	c.Check(err, check.IsNil)

	ch1 := b.Subscribe(1)

	// Notify
	items <- tn
	me := <-ch1
	c.Check(me, check.DeepEquals, tn)

	// Close the main channel
	close(items)
	_, more := <-ch1
	c.Check(more, check.Equals, false)
}

func (s *BroadcasterSuite) TestBroadcasterIP(c *check.C) {
	b := &NotificationBroadcaster{}
	c.Assert(b.IP(), check.Equals, "")

	b.listener = &FakeListener{
		ip: "10.16.17.18",
	}
	c.Assert(b.IP(), check.Equals, "10.16.17.18")
}
