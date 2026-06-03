package pgxelection

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"errors"
	"time"

	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v4/pkg/rselection/electiontypes"
)

// RecoverySuite holds Postgres-free unit tests for the leader's integrity
// check and step-down logic. It intentionally does not use the shared pgx
// pool, so it runs even under `go test -short`.
type RecoverySuite struct{}

var _ = check.Suite(&RecoverySuite{})

func newTestLeader(store ClusterPgStore, nodes map[string]*electiontypes.ClusterNode) *PgxLeader {
	return &PgxLeader{
		store:       store,
		notify:      FakePgNotifier{},
		nodes:       nodes,
		pingSuccess: true,
	}
}

// A node present in memory but absent from the store is pruned, and the check
// then reports healthy.
func (s *RecoverySuite) TestIntegrityPrunesDepartedNode(c *check.C) {
	store := &fakeStore{nodes: map[string]*electiontypes.ClusterNode{
		"a": {Name: "a", IP: "10.0.0.1"},
	}}
	leader := newTestLeader(store, map[string]*electiontypes.ClusterNode{
		"a_10.0.0.1": {Name: "a", IP: "10.0.0.1"},
		"b_10.0.0.2": {Name: "b", IP: "10.0.0.2"}, // not in store; should be pruned
	})

	c.Assert(leader.clusterIntegrityErr(), check.IsNil)
	c.Assert(leader.nodes, check.HasLen, 1)
	_, ok := leader.nodes["b_10.0.0.2"]
	c.Assert(ok, check.Equals, false)
}

// A node present in the store but absent from memory is an error and is NOT
// added to memory (it must be re-added by a real ping response).
func (s *RecoverySuite) TestIntegrityStoreOnlyNodeErrorsWithoutAdding(c *check.C) {
	store := &fakeStore{nodes: map[string]*electiontypes.ClusterNode{
		"a": {Name: "a", IP: "10.0.0.1"},
		"b": {Name: "b", IP: "10.0.0.2"},
	}}
	leader := newTestLeader(store, map[string]*electiontypes.ClusterNode{
		"a_10.0.0.1": {Name: "a", IP: "10.0.0.1"},
	})

	c.Assert(leader.clusterIntegrityErr(), check.NotNil)
	_, ok := leader.nodes["b_10.0.0.2"]
	c.Assert(ok, check.Equals, false)
}

func (s *RecoverySuite) TestIntegrityUnsuccessfulPingErrors(c *check.C) {
	leader := newTestLeader(&fakeStore{}, map[string]*electiontypes.ClusterNode{})
	leader.pingSuccess = false
	c.Assert(leader.clusterIntegrityErr(), check.NotNil)
}

func (s *RecoverySuite) TestIntegrityStoreErrorErrors(c *check.C) {
	store := &fakeStore{err: errors.New("db down")}
	leader := newTestLeader(store, map[string]*electiontypes.ClusterNode{})
	c.Assert(leader.clusterIntegrityErr(), check.NotNil)
}

// healthyLeader returns a leader whose integrity check passes (ping ok, store
// matches memory) with an injectable clock starting at t.
func healthyLeader(t time.Time, stepDown time.Duration) *PgxLeader {
	store := &fakeStore{nodes: map[string]*electiontypes.ClusterNode{
		"a": {Name: "a", IP: "10.0.0.1"},
	}}
	leader := newTestLeader(store, map[string]*electiontypes.ClusterNode{
		"a_10.0.0.1": {Name: "a", IP: "10.0.0.1"},
	})
	leader.stepDownTimeout = stepDown
	cur := t
	leader.now = func() time.Time { return cur }
	leader.setClockTEST = func(nt time.Time) { cur = nt }
	return leader
}

func (s *RecoverySuite) TestStepDownAfterSustainedFailure(c *check.C) {
	start := time.Unix(1_000_000, 0)
	leader := healthyLeader(start, 30*time.Second)

	// Healthy: no step-down, unhealthySince stays zero.
	c.Assert(leader.evaluateHealth(), check.Equals, false)
	c.Assert(leader.unhealthySince.IsZero(), check.Equals, true)

	// Force unhealthy via failing pings.
	leader.pingSuccess = false

	// First failure records the time but does not step down.
	c.Assert(leader.evaluateHealth(), check.Equals, false)
	c.Assert(leader.unhealthySince.Equal(start), check.Equals, true)

	// Still within the timeout window.
	leader.setClockTEST(start.Add(29 * time.Second))
	c.Assert(leader.evaluateHealth(), check.Equals, false)

	// At/after the timeout, step down.
	leader.setClockTEST(start.Add(30 * time.Second))
	c.Assert(leader.evaluateHealth(), check.Equals, true)
}

func (s *RecoverySuite) TestStepDownResetsOnRecovery(c *check.C) {
	start := time.Unix(1_000_000, 0)
	leader := healthyLeader(start, 30*time.Second)

	leader.pingSuccess = false
	c.Assert(leader.evaluateHealth(), check.Equals, false)
	c.Assert(leader.unhealthySince.IsZero(), check.Equals, false)

	// Recover before the timeout elapses.
	leader.pingSuccess = true
	leader.setClockTEST(start.Add(10 * time.Second))
	c.Assert(leader.evaluateHealth(), check.Equals, false)
	c.Assert(leader.unhealthySince.IsZero(), check.Equals, true)

	// A later, separate failure must not immediately trip the timeout.
	leader.pingSuccess = false
	leader.setClockTEST(start.Add(11 * time.Second))
	c.Assert(leader.evaluateHealth(), check.Equals, false)
}

func (s *RecoverySuite) TestStepDownDisabledByZeroTimeout(c *check.C) {
	start := time.Unix(1_000_000, 0)
	leader := healthyLeader(start, 0) // disabled
	leader.pingSuccess = false

	c.Assert(leader.evaluateHealth(), check.Equals, false)
	leader.setClockTEST(start.Add(100 * time.Hour))
	c.Assert(leader.evaluateHealth(), check.Equals, false)
}

func (s *RecoverySuite) TestEvaluateHealthFiresIntegrityCallback(c *check.C) {
	var got []error
	store := &fakeStore{nodes: map[string]*electiontypes.ClusterNode{
		"a": {Name: "a", IP: "10.0.0.1"},
	}}
	leader := newTestLeader(store, map[string]*electiontypes.ClusterNode{
		"a_10.0.0.1": {Name: "a", IP: "10.0.0.1"},
	})
	leader.onIntegrityResult = func(err error) { got = append(got, err) }

	// Healthy check -> callback receives nil.
	leader.evaluateHealth()
	c.Assert(got, check.HasLen, 1)
	c.Assert(got[0], check.IsNil)

	// Unhealthy check -> callback receives a non-nil error.
	leader.pingSuccess = false
	leader.evaluateHealth()
	c.Assert(got, check.HasLen, 2)
	c.Assert(got[1], check.NotNil)
}

func (s *RecoverySuite) TestPingNodesFiresPingCallback(c *check.C) {
	var results []bool
	leader := &PgxLeader{
		address:    "leader",
		chLeader:   "leader",
		chFollower: "follower",
		notify:     FakePgNotifier{},
	}
	leader.onPingResult = func(success bool, _ time.Time) { results = append(results, success) }
	ctx := context.Background()

	// Successful ping.
	leader.pingNodes(ctx)
	c.Assert(results, check.DeepEquals, []bool{true})

	// Failed ping.
	leader.notify = FakePgNotifier{notifyErr: errors.New("down")}
	leader.pingNodes(ctx)
	c.Assert(results, check.DeepEquals, []bool{true, false})
}
