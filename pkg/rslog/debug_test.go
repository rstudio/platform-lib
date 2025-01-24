package rslog

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestDebugSuite(t *testing.T) {
	suite.Run(t, &DebugSuite{})
}

type DebugSuite struct {
	suite.Suite

	defaultLogger Logger
}

func (s *DebugSuite) SetupTest() {
	s.defaultLogger = DefaultLogger()
}

func (s *DebugSuite) TearDownTest() {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	debugRegions = map[ProductRegion]string{}
	debugEnabled = map[ProductRegion]bool{}

	ReplaceDefaultLogger(s.defaultLogger)
}

func (s *DebugSuite) TestProductRegion() {
	inputRegions := map[ProductRegion]string{
		1: "alfa",
		2: "bravo",
		3: "charlie",
	}
	RegisterRegions(inputRegions)

	// Test Regions
	regions := Regions()
	slices.Sort(regions)
	assert.Equal(s.T(), regions, []ProductRegion{1, 2, 3})

	// Test RegionNames
	names := RegionNames()
	slices.Sort(names)
	assert.Equal(s.T(), names, []string{"alfa", "bravo", "charlie"})

	// Test RegionName
	assert.Equal(s.T(), RegionName(1), "alfa")
	assert.Equal(s.T(), RegionName(2), "bravo")
	assert.Equal(s.T(), RegionName(3), "charlie")
	assert.Equal(s.T(), RegionName(4), "")

	// Test RegionByName
	assert.Equal(s.T(), RegionByName("alfa"), ProductRegion(1))
	assert.Equal(s.T(), RegionByName("bravo"), ProductRegion(2))
	assert.Equal(s.T(), RegionByName("charlie"), ProductRegion(3))
	assert.Equal(s.T(), RegionByName("delta"), ProductRegion(0))

	// All registered regions are initially disabled. Test Enabled()
	assert.Equal(s.T(), Enabled(1), false)
	assert.Equal(s.T(), Enabled(2), false)
	assert.Equal(s.T(), Enabled(3), false)

	// Enable a subset. Test InitDebugLogs
	InitDebugLogs([]ProductRegion{1, 3})

	assert.Equal(s.T(), Enabled(1), true)
	assert.Equal(s.T(), Enabled(2), false)
	assert.Equal(s.T(), Enabled(3), true)

	// Flip things around. Test Enable/Disable.
	Disable(1)
	Enable(2)
	Disable(3)

	assert.Equal(s.T(), Enabled(1), false)
	assert.Equal(s.T(), Enabled(2), true)
	assert.Equal(s.T(), Enabled(3), false)
}

func (s *DebugSuite) TestDebugLogger() {
	lgr := NewCapturingLogger(CapturingLoggerOptions{
		Level:        TraceLevel,
		WithMetadata: false,
	})
	ReplaceDefaultLogger(lgr)

	RegisterRegions(map[ProductRegion]string{
		1: "alfa",
		2: "bravo",
		3: "charlie",
	})

	parent := NewDebugLogger(1)
	assert.Equal(s.T(), parent, &debugLogger{
		Logger: lgr.WithFields(Fields{
			"region": "alfa",
		}),
		region: 1,
	})
	subregion := parent.WithSubRegion("drinks")
	assert.Equal(s.T(), subregion, &debugLogger{
		Logger: lgr.WithFields(Fields{
			"region":     "alfa",
			"sub_region": "drinks",
		}),
		region: 1,
	})
	fielded := subregion.WithFields(Fields{
		"kind": "coffee",
	})
	assert.Equal(s.T(), fielded, &debugLogger{
		Logger: lgr.WithFields(Fields{
			"region":     "alfa",
			"sub_region": "drinks",
			"kind":       "coffee",
		}),
		region: 1,
	})

	// Loggers all follow the enabled state for the region.

	// Initially disabled.
	assert.Equal(s.T(), parent.Enabled(), false)
	assert.Equal(s.T(), subregion.Enabled(), false)
	assert.Equal(s.T(), fielded.Enabled(), false)

	// Still disabled when some other region is enabled.
	Enable(2)
	assert.Equal(s.T(), parent.Enabled(), false)
	assert.Equal(s.T(), subregion.Enabled(), false)
	assert.Equal(s.T(), fielded.Enabled(), false)

	// Enabled when the region is enabled.
	Enable(1)
	assert.Equal(s.T(), parent.Enabled(), true)
	assert.Equal(s.T(), subregion.Enabled(), true)
	assert.Equal(s.T(), fielded.Enabled(), true)

	// Still enabled when that other region is disabled
	Disable(2)
	assert.Equal(s.T(), parent.Enabled(), true)
	assert.Equal(s.T(), subregion.Enabled(), true)
	assert.Equal(s.T(), fielded.Enabled(), true)

	// Disabled when the region is disabled again.
	Disable(1)
	assert.Equal(s.T(), parent.Enabled(), false)
	assert.Equal(s.T(), subregion.Enabled(), false)
	assert.Equal(s.T(), fielded.Enabled(), false)

	// When disabled, no messages from debug/trace.
	lgr.Clear()
	parent.Debugf("debug disabled from parent")
	subregion.Debugf("debug disabled from subregion")
	fielded.Debugf("debug disabled from fielded")
	parent.Tracef("trace disabled from parent")
	subregion.Tracef("trace disabled from subregion")
	fielded.Tracef("trace disabled from fielded")
	assert.Empty(s.T(), lgr.Messages())

	// When enabled, messages from debug/trace.
	lgr.Clear()
	Enable(1)
	parent.Debugf("debug enabled from parent")
	subregion.Debugf("debug enabled from subregion")
	fielded.Debugf("debug enabled from fielded")
	parent.Tracef("trace enabled from parent")
	subregion.Tracef("trace enabled from subregion")
	fielded.Tracef("trace enabled from fielded")
	assert.Equal(s.T(), lgr.Messages(), []string{
		"debug enabled from parent",
		"debug enabled from subregion",
		"debug enabled from fielded",
		"trace enabled from parent",
		"trace enabled from subregion",
		"trace enabled from fielded",
	})
}
