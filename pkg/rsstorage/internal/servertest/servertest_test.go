package servertest

// Copyright (C) 2022 by Posit Software, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type ServerTestSuite struct{}

var _ = check.Suite(&ServerTestSuite{})
