package pqlistener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"log"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type PqNotifySuite struct {
	factory *testFactory
	db      *sqlx.DB
}

type testFactory struct{}

func (f *testFactory) NewListener() (*pq.Listener, error) {
	return EphemeralPqListener("postgres"), nil
}

func (f *testFactory) IP() (string, error) {
	return "", nil
}

var _ = check.Suite(&PqNotifySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

func (s *PqNotifySuite) notify(channel string, n *testNotification, c *check.C) {
	err := Notify(channel, n, s.db)
	c.Assert(err, check.IsNil)
}

func (s *PqNotifySuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping pq notification tests because -short was provided")
	}

	s.factory = &testFactory{}

	var err error
	s.db, err = EphemeralPqConn("postgres")
	c.Assert(err, check.IsNil)
}

func (s *PqNotifySuite) TearDownSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping pq notification tests because -short was provided")
	}

	if s.db != nil {
		s.db.Close()
	}
}

func (s *PqNotifySuite) TestNewPqListener(c *check.C) {
	unmarshallers := make(map[uint8]listener.Unmarshaller)
	lgr := &listener.TestLogger{}
	l := NewPqListener("test-a", &testNotification{}, s.factory, unmarshallers, lgr)
	c.Check(l, check.DeepEquals, &PqListener{
		name:          "test-a",
		factory:       s.factory,
		t:             &testNotification{},
		unmarshallers: unmarshallers,
		debugLogger:   lgr,
	})
}

type testNotification struct {
	listener.GenericNotification
	Val string
}

func (s *PqNotifySuite) TestNotificationsNormal(c *check.C) {
	defer leaktest.Check(c)()

	tn := testNotification{
		Val: "test-notification",
	}

	unmarshallers := make(map[uint8]listener.Unmarshaller)

	l := NewPqListener("test-a", &tn, s.factory, unmarshallers, nil)

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count := 0
	go func() {
		defer close(done)
		for {
			select {
			case i := <-data:
				c.Assert(i.(*testNotification).Val, check.Equals, "test-notification")
				count++
				if count == 2 {
					return
				}
			case e := <-errs:
				log.Printf("error received: %s", e)
				c.FailNow()
			}
		}
	}()

	// Send some data across the main test channel.
	s.notify("test-a", &tn, c)
	// Send some data across a different channel. This should not be seen
	s.notify("test-b", &testNotification{Val: "different-test"}, c)
	// Send more data across the main test channel.
	s.notify("test-a", &tn, c)
	c.Assert(err, check.IsNil)

	// Wait for test to complete
	<-done

	// Check IP
	ip := l.IP()
	c.Assert(ip, check.Matches, "")

	// Clean up
	l.Stop()
	// Can call stop multiple times
	l.Stop()

	// Attempt more notifications after stopping. These should not be received.
	s.notify("test-a", &tn, c)
	c.Assert(err, check.IsNil)
	s.notify("test-a", &tn, c)
	c.Assert(err, check.IsNil)

	// Start again, and listen for more notifications
	data, errs, err = l.Listen()
	c.Assert(err, check.IsNil)
	done = make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case i := <-data:
				c.Assert(i.(*testNotification).Val, check.Equals, "second-test")
				return
			case e := <-errs:
				log.Printf("error received: %s", e)
				c.FailNow()
			}
		}
	}()

	// This notification should be received.
	s.notify("test-a", &testNotification{Val: "second-test"}, c)
	c.Assert(err, check.IsNil)

	// Wait for test to complete
	<-done

	// Clean up
	l.Stop()
}

func (s *PqNotifySuite) TestNotificationsBlock(c *check.C) {
	defer leaktest.Check(c)()

	tn := testNotification{
		Val: "test-notification",
	}

	unmarshallers := make(map[uint8]listener.Unmarshaller)

	l := NewPqListener("test-a", &tn, s.factory, unmarshallers, nil)

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count := 0
	blocker := make(chan struct{})
	go func() {
		defer close(done)
		// Block a while before receiving each message
		<-blocker
		log.Printf("Unblocked. Starting to receive.")
		for {
			select {
			case i := <-data:
				c.Assert(i.(*testNotification).Val, check.Equals, "test-notification")
				count++
				if count == 100 {
					return
				}
			case e := <-errs:
				log.Printf("error received: %s", e)
				c.FailNow()
			}
		}
	}()

	// Send some data across the main test channel.
	for i := 0; i < 100; i++ {
		// Send some data across the main test channel.
		s.notify("test-a", &tn, c)
		// Send some data across a different channel. This should not be seen
		s.notify("test-b", &testNotification{Val: "different-test"}, c)
	}

	// Block receiving any notifications until all have been sent
	log.Printf("All messages have been sent")
	close(blocker)

	// Wait for test to complete
	<-done

	// Clean up
	l.Stop()
}
