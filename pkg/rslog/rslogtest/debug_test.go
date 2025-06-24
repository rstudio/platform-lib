package rslogtest

// Copyright (C) 2022 by Posit, PBC.

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

func (s *DebugLoggerSuite) SetupTest() {
	rslog.RegisterRegions(map[rslog.ProductRegion]string{
		LDAP:    "ldap",
		Session: "session",
		OAuth2:  "oauth2",
		Proxy:   "proxy",
		RProc:   "rproc",
		Router:  "router",
	})
}

func (s *DebugLoggerSuite) TestInitLog() {
	loggerMock.On("Infof", "Debug logging enabled for area: %s", mock.Anything).Return(loggerMock)

	s.False(rslog.Enabled(Proxy))

	// Singular region enabled.
	rslog.InitDebugLogs([]rslog.ProductRegion{Proxy})
	s.True(rslog.Enabled(Proxy))
	s.Equal(loggerMock.LastCall(), "Debug logging enabled for area: proxy")
	loggerMock.Clear()

	// multiple regions enabled (using translation and normalization)
	rslog.InitDebugLogs([]rslog.ProductRegion{
		Proxy,
		RProc,
		Router,
	})
	s.True(rslog.Enabled(Proxy))
	s.True(rslog.Enabled(RProc))
	s.True(rslog.Enabled(Router))
	s.Equal(loggerMock.Call(0), "Debug logging enabled for area: proxy")
	s.Equal(loggerMock.Call(1), "Debug logging enabled for area: rproc")
	s.Equal(loggerMock.Call(2), "Debug logging enabled for area: router")
	loggerMock.Clear()

	// calling InitDebugLogs resets what is enabled.
	rslog.InitDebugLogs(nil)
	s.False(rslog.Enabled(Proxy))
	s.False(rslog.Enabled(RProc))
	s.False(rslog.Enabled(Router))
	s.Len(loggerMock.Calls, 0)
	loggerMock.Clear()
}

func (s *DebugLoggerSuite) TestNewDebugLogger() {
	loggerMock.On("Debugf", "test call %s", mock.Anything).Return(loggerMock)

	loggerMock.On("WithFields", rslog.Fields{"region": "proxy"}).Return(loggerMock)
	lgr := rslog.NewDebugLogger(Proxy)
	defer rslog.Disable(Proxy)
	s.Equal(lgr.Enabled(), false)

	rslog.Enable(Proxy)
	lgr.Debugf("test call %s", "base parent")
	s.Equal(lgr.Enabled(), true)
	s.Equal(loggerMock.LastCall(), "test call base parent")

	// Logger with fields should be under same region, new callback
	loggerMock.On("WithFields", rslog.Fields{"id": "654-987"}).Return(loggerMock)
	fieldslgr := lgr.WithFields(rslog.Fields{"id": "654-987"})
	fieldslgr.Debugf("test call %s", "with-fields")
	s.Equal(fieldslgr.Enabled(), true)
	s.Equal(loggerMock.LastCall(), "test call with-fields")

	// SubRegion Logger should be under same region, new callback
	loggerMock.On("WithField", "sub_region", "balancer").Return(loggerMock)
	sublgr := lgr.WithSubRegion("balancer")
	sublgr.Debugf("test call %s", "subregion")
	s.Equal(sublgr.Enabled(), true)
	s.Equal(loggerMock.LastCall(), "test call subregion")

	// Last call, no new calls after disable
	lgr.Debugf("test call %s", "LAST")
	s.Equal(loggerMock.LastCall(), "test call LAST")

	rslog.Disable(Proxy)
	lgr.Debugf("test call %s", "base parent")
	fieldslgr.Debugf("test call %s", "with-fields")
	sublgr.Debugf("test call %s", "subregion")

	s.Equal(loggerMock.LastCall(), "test call LAST")

	// For a totally different region
	loggerMock.On("WithFields", rslog.Fields{"region": "ldap"}).Return(loggerMock)
	another := rslog.NewDebugLogger(LDAP)
	s.Equal(another.Enabled(), false)
}

func (s *DebugLoggerSuite) TestUpdateToLevelAndCaller() {
	loggerMock.On("WithFields", rslog.Fields{"region": "oauth2"}).Return(loggerMock)
	base := &LoggerMock{}
	lgr := rslog.NewDebugLogger(OAuth2)
	lgr.Logger = base

	rslog.Enable(OAuth2)
	s.True(rslog.Enabled(OAuth2))
	base.AssertExpectations(s.T())

	// Sub loggers
	loggerMock.On("WithFields", rslog.Fields{"region": "session"}).Return(loggerMock)
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
