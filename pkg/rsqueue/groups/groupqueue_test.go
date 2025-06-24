package groups

// Copyright (C) 2025 By Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type GroupQueueSuite struct{}

var _ = check.Suite(&GroupQueueSuite{})
