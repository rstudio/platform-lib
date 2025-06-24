package rslogtest

// Copyright (C) 2025 By Posit Software, PBC.

import (
	"log"
	"regexp"
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

func (s *LoggerImplTestSuite) TestNewCapturingLoggerMetadataOption() {
	testCases := []struct {
		Name         string
		WithMetadata bool
		RegexToMatch string
	}{
		{
			Name:         "Test with metadata on",
			WithMetadata: true,
			RegexToMatch: `time="\d{4}\-(0?[1-9]|1[012])\-(0?[1-9]|[12][0-9]|3[01])T\d{2}\:\d{2}\:\d{2}.\d{3}Z" level=info msg=Message field=value`,
		},
		{
			Name:         "Test with metadata off",
			WithMetadata: false,
			RegexToMatch: "Message",
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {

			logger := rslog.NewCapturingLogger(
				rslog.CapturingLoggerOptions{
					Level:        rslog.InfoLevel,
					WithMetadata: tc.WithMetadata,
				},
			)

			logger.WithField("field", "value").Infof("Message")

			log.Println(logger.Messages()[0])
			matched, err := regexp.Match(tc.RegexToMatch, []byte(logger.Messages()[0]))

			s.Nil(err)
			s.True(matched)
		})
	}

}
