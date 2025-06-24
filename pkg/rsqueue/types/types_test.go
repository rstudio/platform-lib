package types

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

type TypesSuite struct{}

var _ = check.Suite(&TypesSuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
