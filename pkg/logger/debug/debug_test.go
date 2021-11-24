package debug

// Copyright (C) 2021 by RStudio, PBC.

import (
	"testing"

	"github.com/rstudio/platform-lib/pkg/logger"
	"github.com/rstudio/platform-lib/pkg/logger/loggertest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type DebugLoggerSuite struct {
	suite.Suite

	loggerMock *loggertest.LoggerMock
}

const (
	Nothing = ProductRegion(iota)
	Proxy
	RProc
	Router
	LDAP
	OAuth2
	Session
)

func init() {
	RegisterRegions(map[ProductRegion]string{})
}

func TestDebugLoggerSuite(t *testing.T) {
	suite.Run(t, &DebugLoggerSuite{})
}

func (s *DebugLoggerSuite) TestInitLog() {
	// Nothing is enabled with an empty set.
	InitLogs(nil)
	s.Len(regionsEnabled, 0)

	// Nothing is enabled with bogus input
	InitLogs([]ProductRegion{Nothing})
	s.Len(regionsEnabled, 0)

	// Singular region enabled.
	InitLogs([]ProductRegion{Proxy})
	s.Equal(regionsEnabled, map[ProductRegion]bool{Proxy: true})

	// multiple regions enabled (using translation and normalization)
	InitLogs([]ProductRegion{Proxy, RProc, Router})
	s.Equal(regionsEnabled,
		map[ProductRegion]bool{
			Proxy:  true,
			RProc:  true,
			Router: true,
		})

	// calling InitLogs resets what is enabled.
	InitLogs(nil)
	s.Len(regionsEnabled, 0)
}

func (s *DebugLoggerSuite) TestNewDebugLogger() {
	s.Len(regionCallbacks, 0)

	lgr := NewDebugLogger(Proxy, logger.DiscardLogger)
	s.Equal(lgr.Enabled(), false)
	s.Len(regionCallbacks, 1)
	s.Len(regionCallbacks[Proxy], 1)

	Enable(Proxy)
	s.Equal(lgr.Enabled(), true)

	// Logger with fields should be under same region, new callback
	fieldslgr := lgr.WithFields(logger.Fields{"id": "654-987"})
	s.Equal(fieldslgr.Enabled(), true)
	s.Len(regionCallbacks, 1)
	s.Len(regionCallbacks[Proxy], 2)

	// SubRegion Logger should be under same region, new callback
	sublgr := lgr.WithSubRegion("balancer")
	s.Equal(sublgr.Enabled(), true)
	s.Len(regionCallbacks, 1)
	s.Len(regionCallbacks[Proxy], 3)

	// For a totally different region
	another := NewDebugLogger(LDAP, logger.DiscardLogger)
	s.Equal(another.Enabled(), false)
	s.Len(regionCallbacks, 2)
	s.Len(regionCallbacks[LDAP], 1)
	s.Len(regionCallbacks[Proxy], 3)
}

func (s *DebugLoggerSuite) TestUpdateToLevelAndCaller() {
	base := &loggertest.LoggerMock{}
	lgr := &debugLogger{
		Logger: base,
		lgr:    base,
		region: OAuth2,
	}

	registerLoggerCb(OAuth2, lgr.enable)
	s.Equal(Enabled(OAuth2), false)

	base.On("SetLevel", logger.DebugLevel)
	base.On("SetReportCaller", true)
	Enable(OAuth2)
	s.Equal(Enabled(OAuth2), true)
	base.AssertExpectations(s.T())

	// Sub loggers
	baseTwo := &loggertest.LoggerMock{}
	lgr = &debugLogger{
		Logger: baseTwo,
		lgr:    baseTwo,
		region: Session,
	}

	registerLoggerCb(Session, lgr.enable)
	s.Equal(Enabled(Session), false)

	baseTwo.On("SetLevel", logger.DebugLevel)
	baseTwo.On("SetReportCaller", true)
	Enable(Session)
	s.Equal(Enabled(Session), true)
	baseTwo.AssertExpectations(s.T())

	baseTwo.On("WithFields", mock.Anything).Return(baseTwo)
	lgr.WithFields(logger.Fields{"sub": "logger"})

	baseTwo.On("SetLevel", logger.ErrorLevel)
	baseTwo.On("SetReportCaller", false)
	Disable(Session)
	s.Equal(Enabled(Session), false)

	// Should have called level AND report caller (2 calls)
	// Should have called with fields for sub logger (3 calls)
	// Should have called level AND report caller for both parent and sub loggers (7 calls)
	s.Len(baseTwo.Calls, 7)
	baseTwo.AssertExpectations(s.T())
}
