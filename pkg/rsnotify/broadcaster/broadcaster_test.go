package broadcaster

// Copyright (C) 2022 by RStudio, PBC.

import (
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rsnotify/listener"
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

// TestUnsubscribeAfterStop verifies that Unsubscribe does not block when called
// after the broadcaster has stopped. This was a bug where the send to the
// unsubscribe channel would block forever if the broadcast loop had exited.
func (s *BroadcasterSuite) TestUnsubscribeAfterStop(c *check.C) {
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

	// Subscribe to get a channel
	ch := b.Subscribe(1)

	// Stop the broadcaster by signaling the stop channel
	stop <- true

	// Wait for the broadcaster to fully stop (stopSignal is closed on exit)
	<-b.stopSignal

	// Now call Unsubscribe after the broadcaster has stopped.
	// This should NOT block - it should return immediately.
	done := make(chan struct{})
	go func() {
		b.Unsubscribe(ch)
		close(done)
	}()

	// If Unsubscribe blocks, this will timeout
	select {
	case <-done:
		// Success - Unsubscribe returned
	case <-time.After(time.Second):
		c.Fatal("Unsubscribe blocked after broadcaster stopped")
	}
}

// TestSubscribeAfterStop verifies that Subscribe does not block when called
// after the broadcaster has stopped, returns nil, and that calling Unsubscribe
// on the nil channel does not leak goroutines.
func (s *BroadcasterSuite) TestSubscribeAfterStop(c *check.C) {
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

	// Stop the broadcaster by signaling the stop channel
	stop <- true

	// Wait for the broadcaster to fully stop (stopSignal is closed on exit)
	<-b.stopSignal

	// Now call Subscribe after the broadcaster has stopped.
	// This should NOT block and should return nil.
	done := make(chan struct{})
	var ch <-chan listener.Notification
	go func() {
		ch = b.Subscribe(1)
		close(done)
	}()

	// If Subscribe blocks, this will timeout
	select {
	case <-done:
		// Success - Subscribe returned
		c.Assert(ch, check.IsNil)
	case <-time.After(time.Second):
		c.Fatal("Subscribe blocked after broadcaster stopped")
	}

	// Unsubscribe(nil) should not block or leak goroutines
	b.Unsubscribe(ch)

	// Also test SubscribeOne
	done2 := make(chan struct{})
	var ch2 <-chan listener.Notification
	go func() {
		ch2 = b.SubscribeOne(1, func(n listener.Notification) bool { return true })
		close(done2)
	}()

	select {
	case <-done2:
		c.Assert(ch2, check.IsNil)
	case <-time.After(time.Second):
		c.Fatal("SubscribeOne blocked after broadcaster stopped")
	}

	// Unsubscribe(nil) should not block or leak goroutines
	b.Unsubscribe(ch2)
}
