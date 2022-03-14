package pgxelection

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rstudio/platform-lib/pkg/rselection"
	"github.com/rstudio/platform-lib/pkg/rselection/electiontypes"
	"github.com/rstudio/platform-lib/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx"
	"gopkg.in/check.v1"
)

type fakeTaskHandler struct {
	verify <-chan chan bool
}

func (f *fakeTaskHandler) Handle(b broadcaster.Broadcaster) {}

func (f *fakeTaskHandler) Register(name string, task rselection.Task) {}

func (f *fakeTaskHandler) Stop() {}

func (f *fakeTaskHandler) Verify() <-chan chan bool {
	return f.verify
}

type FakePgNotifier struct {
	notifyErr error
}

func (f FakePgNotifier) Notify(channel string, msgBytes []byte) error {
	return f.notifyErr
}

func TestPackage(t *testing.T) { check.TestingT(t) }

type LeaderSuite struct {
	dbname string
	pool   *pgxpool.Pool
}

var _ = check.Suite(&LeaderSuite{})

func (s *LeaderSuite) SetUpSuite(c *check.C) {
	if testing.Short() {
		c.Skip("LeaderSuite only runs on Postgres")
	}

	var err error
	s.pool, err = EphemeralPgxPool("postgres")
	c.Assert(err, check.IsNil)
}

func (s *LeaderSuite) TearDownSuite(c *check.C) {
	if testing.Short() {
		c.Skip("LeaderSuite only runs on Postgres")
	}

	s.pool.Close()
}

type fakeStore struct {
	nodes map[string]*electiontypes.ClusterNode
	err   error
}

func (f *fakeStore) Nodes() (map[string]*electiontypes.ClusterNode, error) {
	return f.nodes, f.err
}

func (s *LeaderSuite) TestLeaderLeadStartStopSelfAware(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()

	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePingResponse, &electiontypes.ClusterPingResponse{})
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	matcher.Register(electiontypes.ClusterMessageTypeNodes, &electiontypes.ClusterNodesNotification{})
	plf := postgrespgx.NewPgxListener(channel+"_leader", s.pool, matcher, nil)
	defer plf.Stop()
	awbStop := make(chan bool)
	awb, err := broadcaster.NewNotificationBroadcaster(plf, awbStop)
	defer func() {
		awbStop <- true
	}()
	c.Assert(err, check.IsNil)
	cstore := &fakeStore{
		nodes: map[string]*electiontypes.ClusterNode{
			"one": {
				Name: "one",
				IP:   "192.168.50.10",
			},
			"two": {
				Name: "two",
				IP:   "192.168.50.11",
				Ping: time.Now(),
			},
			"leader_" + awb.IP(): {
				Name: "leader",
				IP:   awb.IP(),
			},
		},
	}
	waitCh := make(chan bool, 2)
	defer close(waitCh)
	notifier := &dummyNotifier{
		msgs: make([]notification, 0),
		wait: waitCh,
	}
	leader := &PgxLeader{
		awb:         awb,
		store:       cstore,
		taskHandler: &fakeTaskHandler{},
		chLeader:    "leader",
		chFollower:  "follower",
		address:     "leader",
		notify:      notifier,
		maxPingAge:  time.Minute,
		ping:        time.Minute,
		sweep:       time.Minute,
		stop:        make(chan bool),

		debugLogger: &fakeLogger{},
		traceLogger: &fakeLogger{},
	}

	// Check key
	c.Assert(leader.Key(), check.Matches, "^leader_\\d+.\\d+.\\d+.\\d+$")

	done := make(chan struct{})
	go func() {
		defer close(done)
		err = leader.Lead()
		c.Assert(err, check.IsNil)
	}()

	// Should have sent one ping
	<-waitCh
	c.Assert(notifier.msgs, check.HasLen, 2)

	// Should have pulled nodes from the store, and should have also added self.
	c.Assert(leader.nodes, check.DeepEquals, map[string]*electiontypes.ClusterNode{
		"one_192.168.50.10": cstore.nodes["one"],
		"two_192.168.50.11": cstore.nodes["two"],
		leader.Key(): {
			Name: leader.address,
			IP:   awb.IP(),
		},
	})

	// Signal and wait for stop
	leader.stop <- true
	<-done
}

func (s *LeaderSuite) TestLeaderLeadInternal(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()
	// Use a real notifier to send the Ping Request notification.
	realNotifier := &PgxPgNotifier{pool: s.pool}
	// Use a fake notifier to record the Ping Response notification.
	nWait := make(chan bool)
	defer close(nWait)
	fakeNotifier := &dummyNotifier{
		msgs: make([]notification, 0),
		wait: nWait,
	}
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePingResponse, &electiontypes.ClusterPingResponse{})
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	matcher.Register(electiontypes.ClusterMessageTypeNodes, &electiontypes.ClusterNodesRequest{})
	plf := postgrespgx.NewPgxListener(channel+"_leader", s.pool, matcher, nil)
	defer plf.Stop()
	awbStop := make(chan bool)
	awb, err := broadcaster.NewNotificationBroadcaster(plf, awbStop)
	c.Assert(err, check.IsNil)
	defer func() {
		awbStop <- true
	}()
	cstore := &fakeStore{}
	loopCh := make(chan bool)
	defer close(loopCh)
	pingCh := make(chan bool)
	defer close(pingCh)
	verifyCh := make(chan chan bool)
	taskHandler := &fakeTaskHandler{
		verify: verifyCh,
	}
	leader := &PgxLeader{
		awb:         awb,
		store:       cstore,
		taskHandler: taskHandler,
		notify:      fakeNotifier,
		chLeader:    "leader",
		chFollower:  "follower",
		address:     "leader",
		nodes: map[string]*electiontypes.ClusterNode{
			"one_192.168.50.11": {
				Name: "one",
				IP:   "192.168.50.11",
			},
			"two_192.168.50.12": {
				Name: "two",
				IP:   "192.168.50.12",
				Ping: time.Now(),
			},
		},
		maxPingAge:         time.Minute,
		loopAwareChTEST:    loopCh,
		pingResponseChTEST: pingCh,
		debugLogger:        &fakeLogger{},
		traceLogger:        &fakeLogger{},
	}

	pingTick := make(chan time.Time)
	sweepTick := make(chan time.Time)
	stop := make(chan bool)
	done := make(chan struct{})
	go func() {
		defer close(done)
		leader.lead(pingTick, sweepTick, stop)
	}()

	// Should preemptively send a ping
	<-nWait
	<-nWait
	c.Assert(fakeNotifier.msgs, check.HasLen, 2)
	c.Assert(leader.nodes, check.HasLen, 2)

	// Send a ping
	pingTick <- time.Now()
	<-nWait
	<-nWait
	c.Assert(fakeNotifier.msgs, check.HasLen, 4)
	c.Assert(leader.nodes, check.HasLen, 2)

	// Sweep
	wait(loopCh, func() { sweepTick <- time.Now() })
	c.Assert(leader.nodes, check.HasLen, 1)

	// Receive notification
	ping := &electiontypes.ClusterPingResponse{
		ClusterNotification: electiontypes.ClusterNotification{
			GuidVal:     "65db0d7d-8db1-4fa8-bc2a-58fad248507f",
			MessageType: electiontypes.ClusterMessageTypePingResponse,
			SrcAddr:     "follower",
			DstAddr:     "leader",
		},
		IP: "192.168.50.11",
	}
	msgBytes, err := json.Marshal(ping)
	c.Assert(err, check.IsNil)
	wait(pingCh, func() {
		err = realNotifier.Notify(channel+"_leader", msgBytes)
		c.Assert(err, check.IsNil)
	})

	// Send a nodes list request
	nodesReq := &electiontypes.ClusterNodesRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			GuidVal:     "65db0d7d-8db1-4fa8-bc2a-58fad248507f",
			MessageType: electiontypes.ClusterMessageTypeNodes,
			SrcAddr:     "follower",
			DstAddr:     "leader",
		},
	}
	msgBytes, err = json.Marshal(nodesReq)
	c.Assert(err, check.IsNil)
	go func() {
		err = realNotifier.Notify(channel+"_leader", msgBytes)
		c.Assert(err, check.IsNil)
	}()
	<-nWait
	c.Assert(fakeNotifier.msgs, check.HasLen, 5)
	// We're not guaranteed on the sort order of the nodes in the response, so simply
	// check for the guid and length.
	c.Assert(string(fakeNotifier.msgs[4].msg), check.Matches, ".+65db0d7d-8db1-4fa8-bc2a-58fad248507f.+")
	c.Assert(len(fakeNotifier.msgs[4].msg), check.Equals, 247)

	// Verify health (store has wrong node count)
	cstore.nodes = map[string]*electiontypes.ClusterNode{
		"follower": {
			Name: "follower",
			IP:   "192.168.50.11",
		},
	}
	vCh := make(chan bool)
	verifyCh <- vCh
	ok := <-vCh
	c.Assert(ok, check.Equals, false)

	// Verify health (store throws error)
	cstore.err = errors.New("store error")
	verifyCh <- vCh
	ok = <-vCh
	c.Assert(ok, check.Equals, false)

	// Verify health (store has wrong nodes)
	cstore.err = nil
	cstore.nodes["three"] = &electiontypes.ClusterNode{}
	verifyCh <- vCh
	ok = <-vCh
	c.Assert(ok, check.Equals, false)

	// Verify health (OK)
	delete(cstore.nodes, "three")
	cstore.nodes["two"] = &electiontypes.ClusterNode{
		Name: "two",
		IP:   "192.168.50.12",
		Ping: time.Now(),
	}

	verifyCh <- vCh
	ok = <-vCh
	c.Assert(ok, check.Equals, true)

	// Stop
	close(stop)

	// Wait for exit
	<-done
}

func (s *LeaderSuite) TestPingNodes(c *check.C) {
	fakeNotifier := FakePgNotifier{}
	leader := &PgxLeader{
		address:    "leader",
		chLeader:   "leader",
		chFollower: "fake",
		notify:     fakeNotifier,

		debugLogger: &fakeLogger{},
		traceLogger: &fakeLogger{},
	}

	leader.pingNodes()
	c.Check(leader.unsuccessfulPing(), check.Equals, false)

	fakeNotifier.notifyErr = errors.New("internet is down")
	leader.notify = fakeNotifier

	leader.pingNodes()
	c.Check(leader.unsuccessfulPing(), check.Equals, true)
}

func (s *LeaderSuite) TestLeaderDemotion(c *check.C) {
	defer leaktest.Check(c)

	channel := c.TestName()
	// Use a real notifier to send the Ping Request notification.
	realNotifier := &PgxPgNotifier{pool: s.pool}
	// Use a fake notifier to record the Ping Response notification.
	fakeNotifier := &dummyNotifier{}
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(electiontypes.ClusterMessageTypePingResponse, &electiontypes.ClusterPingResponse{})
	matcher.Register(electiontypes.ClusterMessageTypePing, &electiontypes.ClusterPingRequest{})
	matcher.Register(electiontypes.ClusterMessageTypeNodes, &electiontypes.ClusterNodesNotification{})
	plf := postgrespgx.NewPgxListener(channel+"_leader", s.pool, matcher, nil)
	defer plf.Stop()
	awbStop := make(chan bool)
	awb, err := broadcaster.NewNotificationBroadcaster(plf, awbStop)
	c.Assert(err, check.IsNil)
	defer func() {
		awbStop <- true
	}()
	stop := make(chan bool)
	leader := &PgxLeader{
		awb:         awb,
		notify:      fakeNotifier,
		chLeader:    "leader",
		chFollower:  "follower",
		address:     "leader",
		stop:        stop,
		taskHandler: &fakeTaskHandler{},
		debugLogger: &fakeLogger{},
		traceLogger: &fakeLogger{},
	}

	// Notified when leader exits
	done := make(chan struct{})
	go func() {
		defer close(done)
		leader.lead(nil, nil, stop)
	}()

	// Receive notification from another leader. This causes the leader to demote itself.
	msgBytes, err := json.Marshal(&electiontypes.ClusterPingRequest{
		ClusterNotification: electiontypes.ClusterNotification{
			GuidVal:     "65db0d7d-8db1-4fa8-bc2a-58fad248507f",
			MessageType: electiontypes.ClusterMessageTypePing,
			SrcAddr:     "another-leader",
			DstAddr:     "leader",
		},
	})
	c.Assert(err, check.IsNil)
	err = realNotifier.Notify(channel+"_leader", msgBytes)
	c.Assert(err, check.IsNil)

	// Wait for exit. Should stop upon self-demotion
	<-done
}

// Waits for the leader loop to advance
func wait(loopCh <-chan bool, do func()) {
	end := make(chan struct{})
	ready := make(chan bool)
	defer close(ready)
	go func() {
		defer close(end)
		for {
			select {
			case <-loopCh:
				return
			case ready <- true:
			}
		}
	}()

	// Wait to ensure we've entered the goroutine loop above,
	<-ready
	// Run the work, and
	do()
	// Wait for the leader loop to advance.
	<-end
}

func (s *LeaderSuite) TestMultiInterfaceLeader(c *check.C) {
	p := &PgxLeader{}

	address := "my-machine"
	p.address = address

	tests := []struct {
		address string
		ip      string

		want bool
	}{
		{address: "", ip: "", want: false},
		{address: "", ip: "127.0.0.1", want: false},
		{address: address, ip: "", want: false},
		{address: address, ip: "1.1.1.1", want: false},
		{address: address, ip: "127.0.0.1", want: true},
	}

	for _, test := range tests {
		c.Check(p.multiInterfaceLeader(test.address, test.ip), check.Equals, test.want)
	}

}
