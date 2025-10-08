package queue

// Copyright (C) 2022 by RStudio, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type QueueSuite struct{}

var _ = check.Suite(&QueueSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
