package rslogtest

// Copyright (C) 2022 by RStudio, PBC.

import (
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
)

func (s *LoggerImplTestSuite) TestNewCapturingLogger() {

	testCases := []struct {
		Name             string
		LogLevel         rslog.LogLevel
		ExpectedMessages []string
	}{
		{
			Name:     "Test Trace Level",
			LogLevel: rslog.TraceLevel,
			ExpectedMessages: []string{
				"Trace Message",
				"Debug Message",
				"Info Message",
				"Warn Message",
				"Error Message",
			},
		},
		{
			Name:     "Test Debug Level",
			LogLevel: rslog.DebugLevel,
			ExpectedMessages: []string{
				"Debug Message",
				"Info Message",
				"Warn Message",
				"Error Message",
			},
		},
		{
			Name:     "Test Info Level",
			LogLevel: rslog.InfoLevel,
			ExpectedMessages: []string{
				"Info Message",
				"Warn Message",
				"Error Message",
			},
		},
		{
			Name:     "Test Warn Level",
			LogLevel: rslog.WarningLevel,
			ExpectedMessages: []string{
				"Warn Message",
				"Error Message",
			},
		},
		{
			Name:     "Test Error Level",
			LogLevel: rslog.ErrorLevel,
			ExpectedMessages: []string{
				"Error Message",
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			logger := rslog.NewCapturingLogger(
				rslog.CapturingLoggerOptions{
					Level:        rslog.InfoLevel,
					WithMetadata: false,
				},
			)

			logger.SetLevel(tc.LogLevel)

			logger.Tracef("Trace Message")
			logger.Debugf("Debug Message")
			logger.Infof("Info Message")
			logger.Warnf("Warn Message")
			logger.Errorf("Error Message")

			s.Assertions.Equal(logger.Messages(), tc.ExpectedMessages)

			logger.Clear()
			s.Assertions.Equal(logger.Messages(), []string{})
		})
	}
}
