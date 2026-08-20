package rsstorage

// Copyright (C) 2026 by Posit Software, PBC

import (
	"gopkg.in/check.v1"
)

// The gocheck entry point for this package lives in server_test.go; suite
// registration below is package-global, so a second check.TestingT here would
// run every suite twice.

type PrefixSuite struct{}

var _ = check.Suite(&PrefixSuite{})

func (s *PrefixSuite) TestItemKey(c *check.C) {
	c.Check(ItemKey("", "GIT_1_2_3.gob"), check.Equals, "GIT_1_2_3.gob")
	c.Check(ItemKey("upsi", "abc"), check.Equals, "upsi/abc")
	c.Check(ItemKey("a/b", "c"), check.Equals, "a/b/c")
}

func (s *PrefixSuite) TestKeyHasPrefix(c *check.C) {
	c.Check(KeyHasPrefix("", "GIT_1_2_3.gob", "GIT_"), check.Equals, true)
	c.Check(KeyHasPrefix("", "GIT_1_2_3.gob", ""), check.Equals, true)
	c.Check(KeyHasPrefix("", "27_GIT_1_2_3.gob", "GIT_"), check.Equals, false)
	c.Check(KeyHasPrefix("upsi", "abc", "GIT_"), check.Equals, false)
	// A prefix need not fall on a path boundary.
	c.Check(KeyHasPrefix("a", "bc", "a/b"), check.Equals, true)
	c.Check(KeyHasPrefix("a", "c", "a/b"), check.Equals, false)
}

// TestSubtreeMayMatch is the pruning contract for a tree walk. Getting this
// wrong in the false direction silently drops results, which is worse than the
// full walk it replaces, so the boundary cases are enumerated explicitly.
func (s *PrefixSuite) TestSubtreeMayMatch(c *check.C) {
	// The root and the empty prefix always qualify.
	c.Check(SubtreeMayMatch("", "GIT_"), check.Equals, true)
	c.Check(SubtreeMayMatch("anything", ""), check.Equals, true)

	// The prefix descends into this directory, so a match may lie deeper.
	c.Check(SubtreeMayMatch("a", "a/b/c"), check.Equals, true)
	c.Check(SubtreeMayMatch("a/b", "a/b/c"), check.Equals, true)

	// The directory is itself inside the prefix, so everything below matches.
	c.Check(SubtreeMayMatch("GIT_x", "GIT_"), check.Equals, true)
	c.Check(SubtreeMayMatch("a", "a"), check.Equals, true)

	// Unrelated subtrees are pruned. This is the case that makes a root-level
	// prefix cheap: none of the cache's real subdirectories can match.
	c.Check(SubtreeMayMatch("upsi", "GIT_"), check.Equals, false)
	c.Check(SubtreeMayMatch("temp", "GIT_"), check.Equals, false)

	// A sibling that merely shares a name prefix with the first path segment
	// must NOT be descended: keys under "ab" are "ab/...", which can never begin
	// with "a/b". Comparing against dir+"/" is what separates this from the
	// "a/bc" case below.
	c.Check(SubtreeMayMatch("ab", "a/b"), check.Equals, false)

	// But a directory whose own name extends the prefix's last segment DOES
	// qualify: keys under "a/bc" are "a/bc/...", which do begin with "a/b".
	c.Check(SubtreeMayMatch("a/bc", "a/b"), check.Equals, true)
}
