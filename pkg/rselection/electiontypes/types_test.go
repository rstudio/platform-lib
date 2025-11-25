package electiontypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"encoding/json"
	"testing"

	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/listener"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type TypesSuite struct {
}

var _ = check.Suite(&TypesSuite{})

func (s *TypesSuite) TestNewClusterNotification(c *check.C) {
	r := NewClusterPingRequest("from-me", "to-you")
	c.Assert(r.Guid(), check.HasLen, 36)
	r.GuidVal = ""
	c.Assert(r, check.DeepEquals, &ClusterPingRequest{
		ClusterNotification: ClusterNotification{
			MessageType: ClusterMessageTypePing,
			SrcAddr:     "from-me",
			DstAddr:     "to-you",
		},
	})
}

func (s *TypesSuite) TestUnmarshalJSONRequestVote(c *check.C) {
	r := NewClusterPingRequest("from-me", "to-you")
	// Fix GUID for deterministic test
	r.GuidVal = "65db0d7d-8db1-4fa8-bc2a-58fad248507f"

	// Marshal to bytes
	b, err := json.Marshal(r)
	c.Assert(err, check.IsNil)
	c.Assert(string(b), check.Equals, "{\"GuidVal\":\"65db0d7d-8db1-4fa8-bc2a-58fad248507f\",\"MessageType\":255,\"SrcAddr\":\"from-me\",\"DstAddr\":\"to-you\"}")
}

func (s *TypesSuite) TestUnmarshalJSONNodesRequest(c *check.C) {
	r := NewClusterNodesRequest()
	// Fix GUID for deterministic test
	r.GuidVal = "65db0d7d-8db1-4fa8-bc2a-58fad248507f"

	// Marshal to bytes
	b, err := json.Marshal(r)
	c.Assert(err, check.IsNil)
	c.Assert(string(b), check.Equals, "{\"GuidVal\":\"65db0d7d-8db1-4fa8-bc2a-58fad248507f\",\"MessageType\":252,\"SrcAddr\":\"\",\"DstAddr\":\"\"}")
}

func (s *TypesSuite) TestClusterNodesNotification(c *check.C) {
	nodes := []ClusterNode{
		{
			Name:   "node1",
			IP:     "192.168.1.1",
			Leader: true,
		},
		{
			Name: "node2",
			IP:   "192.168.1.2",
		},
	}
	cn := NewClusterNodesNotification(nodes, "abc")

	c.Assert(cn, check.DeepEquals, &ClusterNodesNotification{
		GenericNotification: listener.GenericNotification{
			NotifyGuid: "abc",
			NotifyType: ClusterMessageTypeNodesResponse,
		},
		Nodes: nodes,
	})

	// Guid should be set
	c.Assert(cn.Guid(), check.Equals, "abc")
}
