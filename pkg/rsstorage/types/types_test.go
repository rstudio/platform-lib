package types

// Copyright (C) 2025 by Posit Software, PBC

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"gopkg.in/check.v1"
)

func TestPackage(t *testing.T) { check.TestingT(t) }

type TypesSuite struct{}

var _ = check.Suite(&TypesSuite{})

func (s *TypesSuite) TestTypes(c *check.C) {
	usage := Usage{
		SizeBytes:       200 * datasize.GB,
		FreeBytes:       20 * datasize.GB,
		UsedBytes:       180 * datasize.GB,
		CalculationTime: time.Minute,
	}

	c.Assert(usage.String(), check.Equals, "Size: 200GB; Free: 20GB; Used: 180GB; Time: 1m0s")
	c.Assert(usage.ScaleSize(datasize.GB), check.Equals, datasize.ByteSize(200))
	c.Assert(usage.ScaleFree(datasize.GB), check.Equals, datasize.ByteSize(20))
	c.Assert(usage.ScaleUsed(datasize.GB), check.Equals, datasize.ByteSize(180))
}
