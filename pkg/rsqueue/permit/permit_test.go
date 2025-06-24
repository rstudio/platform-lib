package permit

// Copyright (C) 2025 By Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type PermitSuite struct{}

var _ = check.Suite(&PermitSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
