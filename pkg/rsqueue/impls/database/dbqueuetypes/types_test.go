package dbqueuetypes

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type TypesSuite struct{}

func TestPackage(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&TypesSuite{})

func (s *TypesSuite) TestNewQueuePermitExtendNotification(c *check.C) {
	r := NewQueuePermitExtendNotification(456, 8)
	c.Assert(r.Guid(), check.HasLen, 36)
	r.GuidVal = ""
	c.Assert(r, check.DeepEquals, &QueuePermitExtendNotification{
		GuidVal:     "",
		PermitID:    456,
		MessageType: uint8(8),
	})
	c.Assert(r.Guid(), check.Equals, "")
	c.Assert(r.Type(), check.Equals, uint8(8))
}
