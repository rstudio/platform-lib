package agenttypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"testing"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"gopkg.in/check.v1"
)

type AgentTypesSuite struct{}

func TestPackage(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&AgentTypesSuite{})

func (s *AgentTypesSuite) TestWorkCompleteNotification(c *check.C) {
	cn := NewWorkCompleteNotification("some_address", 10)
	// Guid should be set
	c.Assert(cn.Guid(), check.Not(check.Equals), "")
	cn.NotifyGuid = ""
	c.Assert(cn, check.DeepEquals, &WorkCompleteNotification{
		GenericNotification: listener.GenericNotification{
			NotifyType: uint8(10),
		},

		Address: "some_address",
	})
}
