package logging

/* logging_test.go
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

func TestPackage(t *testing.T) { check.TestingT(t) }

type LoggingSuite struct{}

var _ = check.Suite(&LoggingSuite{})

func (s *LoggingSuite) TestLog(c *check.C) {
	l := &DefaultLogger{}
	l.Log("Testing")
	l.Log("Testing %d %s", 2, "now")
}
