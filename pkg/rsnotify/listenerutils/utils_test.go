package listenerutils

/* utils_test.go
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
	"strings"
	"testing"

	"gopkg.in/check.v1"
)

type ListenerUtilsSuite struct{}

var _ = check.Suite(&ListenerUtilsSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }

func (s *ListenerUtilsSuite) TestSafeChannelName(c *check.C) {
	cases := []struct {
		name     string
		expected string
	}{
		{
			name:     "8_CURATED_PACKAGE_ADD_d1cec903-2228-4b71-898d-a1617e3e31bf.gob",
			expected: "8_CURATED_PACKAGE_ADD_d1cec903-2228-4b71-898d-a1617e3e31bf.gob",
		},
		{
			name:     "8_GRAPH_3_222.gob",
			expected: "8_GRAPH_3_222.gob",
		},
		{
			name:     "8_3_1_1_checkpoint_1559062492_c97340db-273e-43c9-89ac-8ddf089a82de.json",
			expected: "0023e24fdf39b0fd8daa2cfbea687f83",
		},
		{
			// at limit for no encoding
			name:     strings.Repeat("|", MaxChannelLen),
			expected: strings.Repeat("|", MaxChannelLen),
		},
		{
			// over limit
			name:     strings.Repeat("|", MaxChannelLen+1),
			expected: "8af2b688da84a75241e1e227afedfd01",
		},
	}
	for _, i := range cases {
		c.Check(SafeChannelName(i.name), check.Equals, i.expected)
	}
}
