package listener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"testing"

	"gopkg.in/check.v1"
)

type NotifySuite struct{}

var _ = check.Suite(&NotifySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

func (s *NotifySuite) TestTypes(c *check.C) {
	c.Assert(NotifyTypeQueue, check.Equals, uint8(1))
	c.Assert(NotifyTypeTx, check.Equals, uint8(2))
	c.Assert(NotifyTypeWorkComplete, check.Equals, uint8(3))
	c.Assert(NotifyTypeSwitchMode, check.Equals, uint8(4))
	c.Assert(NotifyTypeChunk, check.Equals, uint8(5))
}
