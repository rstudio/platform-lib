package listenerfactory

// Copyright (C) 2022 by RStudio, PBC.

import (
	"testing"

	"gopkg.in/check.v1"
)

type ListenerFactorySuite struct{}

var _ = check.Suite(&ListenerFactorySuite{})

func TestPackage(t *testing.T) { check.TestingT(t) }
