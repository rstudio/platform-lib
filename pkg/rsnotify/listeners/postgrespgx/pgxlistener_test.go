package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/jackc/pgx/v4/pgxpool"
	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type PgxNotifySuite struct {
	pool *pgxpool.Pool
}

var _ = check.Suite(&PgxNotifySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

type testNotification struct {
	listener.GenericNotification
	Val string
}

type testNotificationAlt struct {
	listener.GenericNotification
	Val uint64
}

func (s *PgxNotifySuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping pgx notification tests because -short was provided")
	}

	var err error
	s.pool, err = EphemeralPgxPool("postgres")
	c.Assert(err, check.IsNil)
}

func (s *PgxNotifySuite) TearDownSuite(c *check.C) {
	if testing.Short() {
		c.Skip("skipping pgx notification tests because -short was provided")
	}

	s.pool.Close()
}

func (s *PgxNotifySuite) TestNewPgxListener(c *check.C) {
	unmarshallers := make(map[uint8]listener.Unmarshaller)
	matcher := listener.NewMatcher("NotifyType")
	matcher.Register(2, &testNotification{})
	lgr := &listener.TestLogger{}
	l := NewPgxListener("test-a", s.pool, matcher, unmarshallers, lgr)
	c.Check(l, check.DeepEquals, &PgxListener{
		name:          "test-a",
		pool:          s.pool,
		matcher:       matcher,
		unmarshallers: unmarshallers,
		debugLogger:   lgr,
	})
}

func (s *PgxNotifySuite) notify(channel string, n interface{}, c *check.C) {
	err := Notify(channel, n, s.pool)
	c.Assert(err, check.IsNil)
}

func (s *PgxNotifySuite) notifyRaw(channel string, message string, c *check.C) {
	err := NotifyRaw(channel, message, s.pool)
	c.Assert(err, check.IsNil)
}

func (s *PgxNotifySuite) TestNotificationsNormal(c *check.C) {
	defer leaktest.Check(c)()

	matcher := listener.NewMatcher("NotifyType")
	matcher.Register(2, &testNotification{})
	matcher.Register(3, &testNotificationAlt{})

	tn := testNotification{
		GenericNotification: listener.GenericNotification{NotifyType: 2},
		Val:                 "test-notification",
	}

	unmarshallers := make(map[uint8]listener.Unmarshaller)

	l := NewPgxListener("test-a", s.pool, matcher, unmarshallers, nil)

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	count1 := 0
	count2 := 0
	go func() {
		defer close(done)
		for {
			select {
			case i := <-data:
				switch m := i.(type) {
				case *testNotification:
					c.Assert(m.Val, check.Equals, "test-notification")
					count1++
				case *testNotificationAlt:
					c.Assert(m.Val, check.Equals, uint64(999))
					count2++
				}
				if count1 == 2 && count2 == 1 {
					// Return when we've received 2 notifications of *testNotification type
					// and 1 notification of the *testNotificationAlt type
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
	s.notify("test-b", &testNotification{
		GenericNotification: listener.GenericNotification{NotifyType: 2},
		Val:                 "different-test",
	}, c)
	// Send more data across the main test channel.
	s.notify("test-a", &tn, c)
	// Send data of an alternate type across the main test channel
	s.notify("test-a", &testNotificationAlt{
		GenericNotification: listener.GenericNotification{NotifyType: 3},
		Val:                 999,
	}, c)
	c.Assert(err, check.IsNil)

	// Wait for test to complete
	<-done

	// Check IP
	ip := l.IP()
	c.Assert(ip, check.Matches, `^\d+\.\d+\.\d+\.\d+$`)

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
	s.notify("test-a", &testNotification{
		GenericNotification: listener.GenericNotification{NotifyType: 2},
		Val:                 "second-test",
	}, c)
	c.Assert(err, check.IsNil)

	// Wait for test to complete
	<-done

	// Clean up
	l.Stop()
}

func (s *PgxNotifySuite) TestNotificationsErrors(c *check.C) {
	defer leaktest.Check(c)()

	matcher := listener.NewMatcher("NotifyType")
	matcher.Register(2, &testNotification{})

	// A notification that is invalid JSON and cannot be unmarshaled
	tnBytesInvalid := "{!"

	// A notification that does not contain the NotifyType field
	tnNoTypeField := struct {
		Value string
	}{
		Value: "test",
	}

	// A notification whose data type field cannot be unmarshalled to a uint8
	tnBytesInvalidTypeData := `{"NotifyType":{"name":"jon"}}`

	// A notification of an unregistered type
	tnWrongType := testNotification{
		GenericNotification: listener.GenericNotification{NotifyType: 4},
		Val:                 "test-notification",
	}

	// A notification of a registered type that matches a failing marshaller
	tnMarshallerFails := &testNotification{
		GenericNotification: listener.GenericNotification{
			NotifyType: 2,
		},
		Val: "test",
	}

	// A notification with a valid type, but that fails unmarshalling to the expected type
	tnBytesCannotUnmarshal := `{"NotifyType":2,"Val":{"is":"unexpected_object"}}`

	// Register a marshaller that will fail
	unmarshallers := map[uint8]listener.Unmarshaller{
		2: func(n listener.Notification, rawMap map[string]*json.RawMessage) error {
			return errors.New("unmarshal error")
		},
	}

	l := NewPgxListener("test-a", s.pool, matcher, unmarshallers, nil)

	// Listen for notifications
	data, errs, err := l.Listen()
	c.Assert(err, check.IsNil)
	done := make(chan struct{})
	counts := make(map[string]bool)
	go func() {
		defer close(done)
		for {
			select {
			case <-data:
				log.Printf("unexpected good data")
				c.FailNow()
			case e := <-errs:
				errStr := e.Error()
				switch {
				case strings.HasPrefix(errStr, "error unmarshalling raw message"):
					counts["firstUnmarshal"] = true
				case strings.HasPrefix(errStr, "message does not contain data type field NotifyType"):
					counts["missingType"] = true
				case strings.HasPrefix(errStr, "error unmarshalling message data type"):
					counts["badType"] = true
				case strings.HasPrefix(errStr, "no matcher type found for 4"):
					counts["noMatcher"] = true
				case strings.HasPrefix(errStr, "error unmarshalling JSON:"):
					counts["secondUnmarshal"] = true
				case strings.HasPrefix(errStr, "error unmarshalling with custom unmarshaller: unmarshal error"):
					counts["unmarshalerFails"] = true
				}
				if len(counts) == 6 {
					return
				}
			}
		}
	}()

	// Send data across the main test channel. All should err.
	s.notifyRaw("test-a", tnBytesInvalid, c)
	s.notify("test-a", &tnNoTypeField, c)
	s.notifyRaw("test-a", tnBytesInvalidTypeData, c)
	s.notify("test-a", &tnWrongType, c)
	s.notifyRaw("test-a", tnBytesCannotUnmarshal, c)
	s.notify("test-a", &tnMarshallerFails, c)

	// Wait for test to complete
	<-done

	// Clean up
	l.Stop()
}

func (s *PgxNotifySuite) TestNotificationsBlock(c *check.C) {
	defer leaktest.Check(c)()

	matcher := listener.NewMatcher("NotifyType")
	matcher.Register(3, &testNotification{})

	tn := testNotification{
		GenericNotification: listener.GenericNotification{NotifyType: 3},
		Val:                 "test-notification",
	}

	unmarshallers := make(map[uint8]listener.Unmarshaller)

	l := NewPgxListener("test-a", s.pool, matcher, unmarshallers, nil)

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
		s.notify("test-b", &testNotification{
			GenericNotification: listener.GenericNotification{NotifyType: 3},
			Val:                 "different-test",
		}, c)
	}

	// Block receiving any notifications until all have been sent
	log.Printf("All messages have been sent")
	close(blocker)

	// Wait for test to complete
	<-done

	// Clean up
	l.Stop()
}
