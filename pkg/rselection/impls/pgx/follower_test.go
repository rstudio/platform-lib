package pgxelection

// Copyright (C) 2022 by Posit Software, PBC

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rstudio/platform-lib/v2/pkg/rselection/electiontypes"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/postgrespgx"
	"gopkg.in/check.v1"
)

type FollowerSuite struct {
	pool *pgxpool.Pool
}

var _ = check.Suite(&FollowerSuite{})

type fakeBroadcaster struct {
	l  <-chan listener.Notification
	ip string
}

func (f *fakeBroadcaster) Subscribe(dataType uint8) <-chan listener.Notification {
	return f.l
}

func (f *fakeBroadcaster) SubscribeOne(dataType uint8, matcher broadcaster.Matcher) <-chan listener.Notification {
	return f.l
}

func (f *fakeBroadcaster) Unsubscribe(ch <-chan listener.Notification) {
}

func (f *fakeBroadcaster) IP() string {
	return f.ip
}

type addParams struct {
	Item    electiontypes.AssumeLeader
	Address string
}

type fakeQueue struct {
	AddParams []addParams
	PushError error
	Received  chan bool
	Lock      sync.Mutex
}

func generateKey(t time.Time) (key int) {
	t = t.Round(10 * time.Minute)
	key = t.Minute() / 10
	return
}

func (q *fakeQueue) Push(w electiontypes.AssumeLeader) error {
	key := generateKey(time.Now())
	address := fmt.Sprintf("AssumeLeader-%d", key)
	q.AddParams = append(q.AddParams, addParams{w, address})
	q.Lock.Lock()
	defer q.Lock.Unlock()
	if q.Received != nil {
		q.Received <- true
		q.Received = nil
	}
	return q.PushError
}

func (s *FollowerSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("FollowerSuite only runs on Postgres")
	}

	var err error
	s.pool, err = EphemeralPgxPool("postgres")
	c.Assert(err, check.IsNil)
}

func (s *FollowerSuite) TearDownSuite(c *check.C) {
	if testing.Short() {
		c.Skip("FollowerSuite only runs on Postgres")
	}

	s.pool.Close()
}

type notification struct {
	ch  string
	msg []byte
}

type dummyNotifier struct {
	msgs []notification
	wait chan bool
}

func (d *dummyNotifier) Notify(channel string, msgBytes []byte) error {
	d.msgs = append(d.msgs, notification{
		ch:  channel,
		msg: msgBytes,
	})
	if d.wait != nil {
		d.wait <- true
	}
	return nil
}

func (s *FollowerSuite) TestFollowNotify(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()
	q := &fakeQueue{}
	stop := make(chan bool)
	// Use a real notifier to send the Ping Request notification.
	realNotifier := &PgxPgNotifier{pool: s.pool}
	chWait := make(chan bool)
	defer close(chWait)
	// Use a fake notifier to record the Ping Response notification.
	fakeNotifier := &dummyNotifier{
		msgs: make([]notification, 0),
		wait: chWait,
	}
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	plf := postgrespgx.NewPgxListener(postgrespgx.PgxListenerArgs{
		Name:       channel + "_follower",
		Pool:       s.pool,
		Matcher:    matcher,
		IpReporter: postgrespgx.NewPgxIPReporter(s.pool),
	})
	defer plf.Stop()
	awb, err := broadcaster.NewNotificationBroadcaster(plf, stop)
	c.Assert(err, check.IsNil)
	follower := &PgxFollower{
		queue:    q,
		awb:      awb,
		notify:   fakeNotifier,
		chLeader: c.TestName() + "_leader",
		address:  "follower-a",
		stop:     make(chan bool),
		timeout:  time.Minute,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		follower.Follow()
	}()

	// Send a ping
	ping := &electiontypes.ClusterPingRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			GuidVal:     "65db0d7d-8db1-4fa8-bc2a-58fad248507f",
			MessageType: electiontypes.ClusterMessageTypePing,
			SrcAddr:     "leader",
		},
	}
	msgBytes, err := json.Marshal(ping)
	c.Assert(err, check.IsNil)
	err = realNotifier.Notify(channel+"_follower", msgBytes)
	c.Assert(err, check.IsNil)

	// Wait for notification to be handled
	<-chWait

	// Signal and wait for stop
	follower.stop <- true
	<-done

	// Check for recorded ping response. The follower should have
	// received a ping request and responded with a ping response.
	c.Assert(fakeNotifier.msgs, check.HasLen, 1)
	c.Assert(fakeNotifier.msgs[0].ch, check.Equals, channel+"_leader")
	c.Assert(string(fakeNotifier.msgs[0].msg), check.Matches,
		`{"GuidVal":".+","MessageType":250,"SrcAddr":"follower-a","DstAddr":"leader","IP":".+"}`)
}

func (s *FollowerSuite) TestFollowPromote(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()
	q := &fakeQueue{}
	stop := make(chan bool)
	// Use a fake notifier to record the Ping Response notification.
	fakeNotifier := &dummyNotifier{
		msgs: make([]notification, 0),
	}
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	plf := postgrespgx.NewPgxListener(postgrespgx.PgxListenerArgs{
		Name:       channel + "_follower",
		Pool:       s.pool,
		Matcher:    matcher,
		IpReporter: postgrespgx.NewPgxIPReporter(s.pool),
	})
	defer plf.Stop()
	awb, err := broadcaster.NewNotificationBroadcaster(plf, stop)
	c.Assert(err, check.IsNil)
	follower := &PgxFollower{
		queue:    q,
		awb:      awb,
		notify:   fakeNotifier,
		chLeader: c.TestName() + "_leader",
		address:  "follower-a",
		stop:     make(chan bool),
		timeout:  time.Minute,
		promote:  make(chan bool),
	}

	done := make(chan struct{})
	var result FollowResult
	go func() {
		defer close(done)
		result = follower.Follow()
	}()

	// Request promotion
	follower.Promote()
	<-done

	c.Assert(result, check.Equals, FollowResultPromote)
}

func (s *FollowerSuite) TestFollowRequestLeader(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()
	received := make(chan bool)
	defer close(received)
	q := &fakeQueue{
		Received: received,
	}
	stop := make(chan bool)
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	plf := postgrespgx.NewPgxListener(postgrespgx.PgxListenerArgs{
		Name:       channel + "_follower",
		Pool:       s.pool,
		Matcher:    matcher,
		IpReporter: postgrespgx.NewPgxIPReporter(s.pool),
	})
	defer plf.Stop()
	awb, err := broadcaster.NewNotificationBroadcaster(plf, stop)
	c.Assert(err, check.IsNil)
	follower := &PgxFollower{
		queue:    q,
		awb:      awb,
		chLeader: c.TestName() + "_leader",
		address:  "follower-a",
		stop:     make(chan bool),
		timeout:  time.Nanosecond,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		follower.Follow()
	}()

	// Wait for work to be pushed into the queue.
	<-q.Received

	// Signal and wait for stop
	follower.stop <- true
	<-done

	// Check for recorded AssumeLeader work in the queue. There should be
	// at least one, but likely more than one due to the short timeout.
	c.Assert(len(q.AddParams) > 0, check.Equals, true)
	c.Assert(q.AddParams[0].Address, check.Matches, "AssumeLeader-[0-5]")
	c.Assert(q.AddParams[0].Item, check.DeepEquals, electiontypes.AssumeLeader{
		SrcAddr: "follower-a",
	})
}

func (s *FollowerSuite) TestHandleNotify(c *check.C) {
	defer leaktest.Check(c)

	notifier := &dummyNotifier{
		msgs: make([]notification, 0),
	}
	awb := &fakeBroadcaster{ip: "192.168.5.11"}
	follower := &PgxFollower{
		notify:   notifier,
		chLeader: c.TestName() + "_leader",
		address:  "follower-a",
		awb:      awb,
	}
	follower.handleNotify(&electiontypes.ClusterPingRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			GuidVal:     "65db0d7d-8db1-4fa8-bc2a-58fad248507f",
			MessageType: electiontypes.ClusterMessageTypePing,
			SrcAddr:     "leader",
			DstAddr:     "follower",
		},
	})
	c.Assert(notifier.msgs, check.HasLen, 1)
	c.Assert(notifier.msgs[0].ch, check.Equals, follower.chLeader)
	c.Assert(string(notifier.msgs[0].msg), check.Matches, `{"GuidVal":".+","MessageType":250,"SrcAddr":"follower-a","DstAddr":"leader","IP":"192.168.5.11"}`)
}

func (s *FollowerSuite) TestRequestLeader(c *check.C) {
	defer leaktest.Check(c)

	q := &fakeQueue{}
	follower := &PgxFollower{
		queue:   q,
		address: "follower-a",
	}

	follower.requestLeader()
	c.Assert(q.AddParams, check.HasLen, 1)
	c.Assert(q.AddParams[0].Address, check.Matches, "AssumeLeader-[0-5]")
	c.Assert(q.AddParams[0].Item, check.DeepEquals, electiontypes.AssumeLeader{
		SrcAddr: "follower-a",
	})
}

func EphemeralPgxPool(dbname string) (*pgxpool.Pool, error) {
	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, err
	}

	config.MaxConns = int32(10)

	return pgxpool.NewWithConfig(context.Background(), config)
}
