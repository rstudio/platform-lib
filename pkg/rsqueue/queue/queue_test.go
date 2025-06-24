package queue

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type QueueSuite struct{}

var _ = check.Suite(&QueueSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
