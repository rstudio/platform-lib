package debug_test

// Copyright (C) 2021 by RStudio, PBC.

import (
	"testing"

	"github.com/rstudio/platform-lib/pkg/logger"
	"github.com/rstudio/platform-lib/pkg/logger/debug"
	"github.com/rstudio/platform-lib/pkg/logger/loggertest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// type DebugLoggerSuite struct {
// 	suite.Suite

// 	loggerMock *loggertest.LoggerMock
// }

const (
	Nothing = debug.ProductRegion(iota)
	Proxy
	RProc
	Router
	LDAP
	OAuth2
	Session
)

func init() {
	debug.RegisterRegions(map[debug.ProductRegion]string{})
}

type DebugLoggerSuite struct {
	suite.Suite

	loggerMock *loggertest.LoggerMock
}

func TestDebugLoggerSuite(t *testing.T) {
	suite.Run(t, &DebugLoggerSuite{})
}

func (s *DebugLoggerSuite) TestInitLog() {
	s.False(debug.Enabled(Proxy))

	// Singular region enabled.
	debug.InitLogs([]debug.ProductRegion{Proxy})
	s.True(debug.Enabled(Proxy))

	// multiple regions enabled (using translation and normalization)
	debug.InitLogs([]debug.ProductRegion{
		Proxy,
		RProc,
		Router,
	})
	s.True(debug.Enabled(Proxy))
	s.True(debug.Enabled(RProc))
	s.True(debug.Enabled(Router))

	// calling InitLogs resets what is enabled.
	debug.InitLogs(nil)
	s.False(debug.Enabled(Proxy))
	s.False(debug.Enabled(RProc))
	s.False(debug.Enabled(Router))
}

func (s *DebugLoggerSuite) TestNewDebugLogger() {
	lgr := debug.NewDebugLogger(Proxy, logger.DiscardLogger)
	s.Equal(lgr.Enabled(), false)

	debug.Enable(Proxy)
	s.Equal(lgr.Enabled(), true)

	// Logger with fields should be under same region, new callback
	fieldslgr := lgr.WithFields(logger.Fields{"id": "654-987"})
	s.Equal(fieldslgr.Enabled(), true)

	// SubRegion Logger should be under same region, new callback
	sublgr := lgr.WithSubRegion("balancer")
	s.Equal(sublgr.Enabled(), true)

	// For a totally different region
	another := debug.NewDebugLogger(LDAP, logger.DiscardLogger)
	s.Equal(another.Enabled(), false)
}

func (s *DebugLoggerSuite) TestUpdateToLevelAndCaller() {
	base := &loggertest.LoggerMock{}
	lgr := debug.NewDebugLogger(OAuth2, logger.DiscardLogger)
	lgr.Logger = base

	base.On("SetLevel", logger.DebugLevel)
	base.On("SetReportCaller", true)
	debug.Enable(OAuth2)
	s.True(debug.Enabled(OAuth2))
	base.AssertExpectations(s.T())

	// Sub loggers
	baseTwo := &loggertest.LoggerMock{}
	lgr = debug.NewDebugLogger(Session, logger.DiscardLogger)
	lgr.Logger = baseTwo

	baseTwo.On("SetLevel", logger.DebugLevel)
	baseTwo.On("SetReportCaller", true)
	debug.Enable(Session)
	s.True(debug.Enabled(Session))
	baseTwo.AssertExpectations(s.T())

	baseTwo.On("WithFields", mock.Anything).Return(baseTwo)
	lgr.WithFields(logger.Fields{"sub": "logger"})

	baseTwo.On("SetLevel", logger.ErrorLevel)
	baseTwo.On("SetReportCaller", false)
	debug.Disable(Session)
	s.False(debug.Enabled(Session))

	// Should have called level AND report caller (2 calls)
	// Should have called with fields for sub logger (3 calls)
	// Should have called level AND report caller for both parent and sub loggers (7 calls)
	s.Len(baseTwo.Calls, 7)
	baseTwo.AssertExpectations(s.T())
}
