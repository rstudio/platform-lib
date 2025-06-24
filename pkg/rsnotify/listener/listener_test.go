package listener

// Copyright (C) 2022 by Posit, PBC.

import (
	"testing"

	"gopkg.in/check.v1"
)

type ListenerSuite struct{}

func TestPackage(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&ListenerSuite{})
