package metrics

// Copyright (C) 2022 by RStudio, PBC

import (
	"testing"

	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type CarrierSuite struct{}

var _ = check.Suite(&CarrierSuite{})
