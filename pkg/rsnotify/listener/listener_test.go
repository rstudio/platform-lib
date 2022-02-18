package listener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"testing"

	"gopkg.in/check.v1"
)

type NotifySuite struct{}

var _ = check.Suite(&NotifySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
