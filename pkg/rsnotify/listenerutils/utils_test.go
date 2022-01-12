package listenerutils

// Copyright (C) 2022 by RStudio, PBC.

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
