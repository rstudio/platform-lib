package rselection

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type LeaderSuite struct{}

var _ = check.Suite(&LeaderSuite{})
