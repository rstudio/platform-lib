package rslogtest

// Copyright (C) 2021 by RStudio, PBC.

import (
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	Nothing = rslog.ProductRegion(iota)
	Proxy
	RProc
	Router
	LDAP
	OAuth2
	Session
)

type DebugLoggerSuite struct {
	suite.Suite
}

func TestDebugLoggerSuite(t *testing.T) {
	suite.Run(t, &DebugLoggerSuite{})
}

func (s *DebugLoggerSuite) TestInitLog() {
	s.False(rslog.Enabled(Proxy))

	// Singular region enabled.
	rslog.InitLogs([]rslog.ProductRegion{Proxy})
	s.True(rslog.Enabled(Proxy))

	// multiple regions enabled (using translation and normalization)
	rslog.InitLogs([]rslog.ProductRegion{
		Proxy,
		RProc,
		Router,
	})
	s.True(rslog.Enabled(Proxy))
	s.True(rslog.Enabled(RProc))
	s.True(rslog.Enabled(Router))

	// calling InitLogs resets what is enabled.
	rslog.InitLogs(nil)
	s.False(rslog.Enabled(Proxy))
	s.False(rslog.Enabled(RProc))
	s.False(rslog.Enabled(Router))
}

func (s *DebugLoggerSuite) TestNewDebugLogger() {
	loggerMock.On("WithFields", rslog.Fields{"region": ""}).Return(loggerMock)
	loggerMock.On("WithFields", rslog.Fields{"id": "654-987"}).Return(loggerMock)
	loggerMock.On("WithField", "sub_region", "balancer").Return(loggerMock)

	lgr := rslog.NewDebugLogger(Proxy)
	defer rslog.Disable(Proxy)
	s.Equal(lgr.Enabled(), false)

	rslog.Enable(Proxy)
	s.Equal(lgr.Enabled(), true)

	// Logger with fields should be under same region, new callback
	fieldslgr := lgr.WithFields(rslog.Fields{"id": "654-987"})
	s.Equal(fieldslgr.Enabled(), true)

	// SubRegion Logger should be under same region, new callback
	sublgr := lgr.WithSubRegion("balancer")
	s.Equal(sublgr.Enabled(), true)

	// For a totally different region
	another := rslog.NewDebugLogger(LDAP)
	s.Equal(another.Enabled(), false)
}

func (s *DebugLoggerSuite) TestUpdateToLevelAndCaller() {
	base := &LoggerMock{}
	lgr := rslog.NewDebugLogger(OAuth2)
	lgr.Logger = base

	rslog.Enable(OAuth2)
	s.True(rslog.Enabled(OAuth2))
	base.AssertExpectations(s.T())

	// Sub loggers
	baseTwo := &LoggerMock{}
	lgr = rslog.NewDebugLogger(Session)
	lgr.Logger = baseTwo

	rslog.Enable(Session)
	s.True(rslog.Enabled(Session))
	baseTwo.AssertExpectations(s.T())

	baseTwo.On("WithFields", mock.Anything).Return(baseTwo)
	lgr.WithFields(rslog.Fields{"sub": "logger"})

	rslog.Disable(Session)
	s.False(rslog.Enabled(Session))

	// Should have called with fields for sub logger (1 call)
	s.Len(baseTwo.Calls, 1)
	baseTwo.AssertExpectations(s.T())
}
