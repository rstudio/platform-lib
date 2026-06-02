package pgxelection

// Copyright (C) 2026 by Posit Software, PBC

import (
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

var _ = time.Now // retained; used by later step-down tests in this file
