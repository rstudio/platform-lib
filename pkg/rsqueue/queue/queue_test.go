package queue

// Copyright (C) 2022 by RStudio, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type QueueSuite struct{}

var _ = check.Suite(&QueueSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

type TypesSuite struct{}

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
