package rslogtest

// Copyright (C) 2022 by RStudio, PBC.

import (
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type BufLoggerTestSuite struct {
	suite.Suite
}

func TestBufLoggerSuite(t *testing.T) {
	suite.Run(t, &BufLoggerTestSuite{})
}

type flushLogger struct {
	mock.Mock
}

func (f *flushLogger) Flush() {
	f.Called()
}

func (s *BufLoggerTestSuite) TestBuffering() {
	coreLogger := &LoggerMock{}
	methods := []string{"Errorf", "Warnf", "Debugf", "Infof", "Tracef"}
	coreLogger.AllowAny(methods...)

	bufLogger := rslog.NewBufLogger(coreLogger)
	bufLogger.Debugf("testing %s", "debug")
	bufLogger.Errorf("testing %s", "error")
	bufLogger.Infof("testing %s", "info")
	bufLogger.Warnf("testing %s", "warn")
	bufLogger.Tracef("testing %s", "trace")

	for _, m := range methods {
		s.T().Run(m, func(t *testing.T) {
			coreLogger.AssertNotCalled(t, m, mock.Anything)
		})
	}

	s.Assert().Equal(
		bufLogger.Storage.Logs,
		[]rslog.BufLogEntry{
			{
				Level:   rslog.DebugLevel,
				Message: "testing %s",
				Args:    []interface{}{"debug"},
				Logger:  coreLogger,
			},
			{
				Level:   rslog.ErrorLevel,
				Message: "testing %s",
				Args:    []interface{}{"error"},
				Logger:  coreLogger,
			},
			{
				Level:   rslog.InfoLevel,
				Message: "testing %s",
				Args:    []interface{}{"info"},
				Logger:  coreLogger,
			},
			{
				Level:   rslog.WarningLevel,
				Message: "testing %s",
				Args:    []interface{}{"warn"},
				Logger:  coreLogger,
			},
			{
				Level:   rslog.TraceLevel,
				Message: "testing %s",
				Args:    []interface{}{"trace"},
				Logger:  coreLogger,
			},
		},
	)
}

func (s *BufLoggerTestSuite) TestPanics() {
	f := &flushLogger{}
	lgr := &LoggerMock{}

	bufLogger := rslog.NewBufLogger(struct {
		rslog.CoreLoggerImpl
		rslog.Flusher
	}{lgr, f})

	lgr.On("Fatal", mock.Anything)
	f.On("Flush")
	bufLogger.Fatal("testing fatal")
	lgr.AssertCalled(s.T(), "Fatal", []interface{}{"testing fatal"})
	f.AssertCalled(s.T(), "Flush")
	lgr.ExpectedCalls = nil
	f.ExpectedCalls = nil

	lgr.On("Fatalf", mock.AnythingOfType("string"), mock.Anything)
	f.On("Flush")
	bufLogger.Fatalf("testing %s", "fatalf")
	lgr.AssertCalled(s.T(), "Fatalf", "testing %s", []interface{}{"fatalf"})
	f.AssertCalled(s.T(), "Flush")
	lgr.ExpectedCalls = nil
	f.ExpectedCalls = nil

	lgr.On("Panicf", mock.AnythingOfType("string"), mock.Anything)
	f.On("Flush")
	bufLogger.Panicf("testing %s", "panicf")
	lgr.AssertCalled(s.T(), "Panicf", "testing %s", []interface{}{"panicf"})
	f.AssertCalled(s.T(), "Flush")
	lgr.ExpectedCalls = nil
	f.ExpectedCalls = nil
}
