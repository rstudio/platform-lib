package local

// Copyright (C) 2025 By Posit Software, PBC.

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
)

type LocalNotifySuite struct {
}

var _ = check.Suite(&LocalNotifySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

func (s *LocalNotifySuite) TestNewLocalListener(c *check.C) {
	f := NewListenerProvider(ListenerProviderArgs{})
	l := f.New("test-a")
	c.Check(l.guid, check.HasLen, 36)
	l.guid = ""
	c.Check(l, check.DeepEquals, &Listener{
		name:     "test-a",
		provider: f,
	})
}

func (s *LocalNotifySuite) TestNotifications(c *check.C) {
	defer leaktest.Check(c)()

	f := NewListenerProvider(ListenerProviderArgs{})
	l := f.New("test-a")

	tn := listener.TestNotification{
		Val: "test-notification",
	}

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count := 0
	var testError error
	go func() {
		defer close(done)
		for {
			select {
			case i := <-data:
				if i.(*listener.TestNotification).Val != "test-notification" {
					testError = fmt.Errorf("invalid value received: %s", i.(*listener.TestNotification).Val)
					return
				}
				count++
				if count == 2 {
					return
				}
			case e := <-errs:
				log.Printf("error received: %s", e)
				testError = e
				return
			}
		}
	}()

	// Send some data across the main test channel.
	f.Notify("test-a", &tn)
	// Send some data across a different channel. This should not be seen
	f.Notify("test-bb", &listener.TestNotification{Val: "different-test"})
	// Send more data across the main test channel.
	f.Notify("test-a", &tn)

	// Wait for test to complete
	<-done
	c.Assert(testError, check.IsNil)

	// Clean up
	l.Stop()
	// Can call stop multiple times
	l.Stop()

	// Attempt more notifications after stopping. These should not be received.
	f.Notify("test-a", &tn)
	c.Assert(err, check.IsNil)
	f.Notify("test-a", &tn)
	c.Assert(err, check.IsNil)

	// Start again, and listen for more notifications
	data, errs, err = l.Listen()
	c.Assert(err, check.IsNil)

	// Start another listener
	l2 := f.New("test-bb")
	data2, errs2, err := l2.Listen()

	// List for events on the "test-a" channel
	done = make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case i := <-data:
				c.Assert(i.(*listener.TestNotification).Val, check.Equals, "second-test")
				return
			case e := <-errs:
				log.Printf("error received: %s", e)
				c.FailNow()
			}
		}
	}()

	// List for events on the "test-bb" channel
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		for {
			select {
			case i := <-data2:
				c.Assert(i.(*listener.TestNotification).Val, check.Equals, "second-test-bb")
				return
			case e := <-errs2:
				log.Printf("error received: %s", e)
				c.FailNow()
			}
		}
	}()

	// This notification should be received.
	f.Notify("test-a", &listener.TestNotification{Val: "second-test"})
	c.Assert(err, check.IsNil)

	// This notification should not be received.
	f.Notify("test-zz", &listener.TestNotification{Val: "second-test-not-received"})
	c.Assert(err, check.IsNil)

	// This notification should be received.
	f.Notify("test-bb", &listener.TestNotification{Val: "second-test-bb"})
	c.Assert(err, check.IsNil)

	// Wait for tests to complete
	<-done
	<-done2

	// Clean up
	c.Assert(f.listeners, check.HasLen, 2)
	l.Stop()
	c.Assert(f.listeners, check.HasLen, 1)
	l2.Stop()

	c.Assert(f.listeners, check.HasLen, 0)
}

func (s *LocalNotifySuite) TestNotificationsBlock(c *check.C) {
	defer leaktest.Check(c)()

	f := NewListenerProvider(ListenerProviderArgs{})
	l := f.New("test-a")

	tn := listener.TestNotification{
		Val: "test-notification",
	}

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count := 0
	var testError error
	blocker := make(chan struct{})
	go func() {
		defer close(done)
		// Block a while before receiving each message
		<-blocker
		log.Printf("Unblocked. Starting to receive.")
		for {
			select {
			case i := <-data:
				if i.(*listener.TestNotification).Val != "test-notification" {
					testError = fmt.Errorf("invalid value received: %s", i.(*listener.TestNotification).Val)
					return
				}
				count++
				if count == 100 {
					return
				}
			case e := <-errs:
				log.Printf("error received: %s", e)
				testError = e
				return
			}
		}
	}()

	// Send some data across the main test channel.
	for i := 0; i < 100; i++ {
		f.Notify("test-a", &tn)
		// Send some data across a different channel. This should not be seen
		f.Notify("test-bb", &listener.TestNotification{Val: "different-test"})
	}

	// Block receiving any notifications until all have been sent
	log.Printf("All messages have been sent")
	c.Assert(f.listeners, check.HasLen, 1)
	close(blocker)

	// Wait for test to complete
	<-done
	c.Assert(testError, check.IsNil)

	// Clean up
	l.Stop()

	c.Assert(f.listeners, check.HasLen, 0)
}

func (s *LocalNotifySuite) TestNotificationsErrs(c *check.C) {
	defer leaktest.Check(c)()

	f := NewListenerProvider(ListenerProviderArgs{})
	l := f.New("test-a")

	tn := listener.TestNotification{
		Val: "test-notification",
	}

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count := 0
	errCount := 0
	var testError error
	blocker := make(chan struct{})
	go func() {
		defer close(done)
		// Block a while before receiving each message
		<-blocker
		log.Printf("Unblocked. Starting to receive.")
		for {
			select {
			case i := <-data:
				if i.(*listener.TestNotification).Val != "test-notification" {
					testError = fmt.Errorf("invalid value received: %s", i.(*listener.TestNotification).Val)
					return
				}
				count++
			case e := <-errs:
				log.Printf("error received: %s", e)
				if e.Error() != "a notification must be of type listener.Notification" {
					testError = fmt.Errorf("invalid error received: %s", e.Error())
					return
				}
				errCount++
				return
			}
			if count == 1 && errCount == 1 {
				return
			}
		}
	}()

	// Send some data across the main test channel.
	f.Notify("test-a", &tn)
	f.Notify("test-a", struct{ name string }{"testA"})

	// Block receiving any notifications until all have been sent
	log.Printf("All messages have been sent")
	close(blocker)

	// Wait for test to complete
	<-done
	c.Assert(testError, check.IsNil)

	// Clean up
	l.Stop()

	c.Assert(f.listeners, check.HasLen, 0)
}

func (s *LocalNotifySuite) TestNotificationsDeadlockOnClose(c *check.C) {
	defer leaktest.Check(c)()

	// When handling addressed queue work, there's a chance that `PollAddress`
	// may find the completed work before a notification is received that the
	// work is complete. When this happens, then we call `addressListener.Stop()`
	// to close the listener. If the notification that work is received is
	// received by the listener but can't be sent to the `items` channel before
	// we stop listening, a deadlock can occur. This test was introduced to
	// catch the deadlock and ensure that we don't regress.

	f := NewListenerProvider(ListenerProviderArgs{})
	// Shorten timeout
	f.notifyTimeout = 50 * time.Millisecond
	l := f.New("test-deadlock")

	// We won't start selecting on the channels in l.wait() until `starter` is
	// notified
	starter := make(chan struct{})
	l.deferredStart = starter

	tn := listener.TestNotification{
		Val: "test-notification",
	}

	// Start listening
	_, _, err := l.Listen()
	c.Assert(err, check.IsNil)

	// Send some data across the main test channel. Do this in a separate goroutine
	// so it doesn't block. Since we have not yet notified/closed `starter` (see
	// a few lines above), the `Notify` call will block.
	done := make(chan struct{})
	var testError error
	go func() {
		defer close(done)
		log.Printf("ready to notify")
		f.Notify("test-deadlock", &tn)
		log.Printf("done notifying")
	}()

	// Stop the channel. This will block until we start listening, below. The idea
	// here is to stop the channel before the listener has a chance to receive the
	// notification we've sent.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		l.Stop()
	}()

	// Now that the channel is being stopped, tell the listener it's time to listen.
	// Since we're stopping the channel, the notification attempt should time out and
	// be re-attempted. However, the listener is gone by the second attempt and isn't
	// retried.
	log.Printf("notifying start channel that wait can proceed")
	close(starter)

	// Make sure we know we stopped
	<-stopped
	c.Assert(f.listeners, check.HasLen, 0)
	log.Printf("Stop completed. No listeners")

	// Finally, wait for the notification to complete. The timeout/retry mechanism
	// ensures that we don't block here forever.
	<-done
	c.Assert(testError, check.IsNil)
}
