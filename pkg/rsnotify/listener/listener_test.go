package listener

/* listener_test.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

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
