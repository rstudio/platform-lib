package rslogtest

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var loggerMock *LoggerMock

func init() {
	loggerMock = &LoggerMock{}
	rslog.DefaultLoggerFactory = &mockFactory{logger: loggerMock}
	rslog.RegisterRegions(map[rslog.ProductRegion]string{})
}

type LoggerImplTestSuite struct {
	suite.Suite
}

func TestLoggerImplSuite(t *testing.T) {
	suite.Run(t, &LoggerImplTestSuite{})
}

var outputDest = []rslog.OutputDest{
	{
		Output:      rslog.LogOutputFile,
		Filepath:    "/custom/dir/server.log",
		DefaultFile: "/var/log/rstudio/rstudio-xyz/rstudio-xyz.log",
	},
}

func (s *LoggerImplTestSuite) TestNewLoggerImpl() {
	outputMock := &OutputBuilderMock{}
	expectedOutput := &IoWriterMock{}
	outputMock.On("Build", outputDest).Return(expectedOutput, nil)

	result, err := rslog.NewLoggerImpl(
		rslog.LoggerOptionsImpl{
			Output: outputDest,
			Format: rslog.TextFormat,
			Level:  rslog.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.Equal(expectedOutput, result.CoreLogger.Out)
	s.IsType(&rslog.UTCTextFormatter{}, result.CoreLogger.Formatter)

	result, err = rslog.NewLoggerImpl(
		rslog.LoggerOptionsImpl{
			Output: outputDest,
			Format: rslog.JSONFormat,
			Level:  rslog.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.IsType(&rslog.UTCJSONFormatter{}, result.CoreLogger.Formatter)

	result, err = rslog.NewLoggerImpl(
		rslog.LoggerOptionsImpl{
			Output: outputDest,
			Format: rslog.OutputFormat("UnsupportedFormat"),
			Level:  rslog.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.IsType(&rslog.UTCTextFormatter{}, result.CoreLogger.Formatter)

	errdBuildMock := &OutputBuilderMock{}
	errdBuildMock.On("Build", outputDest).Return(IoWriterMock{}, fmt.Errorf("output build error"))
	result, err = rslog.NewLoggerImpl(
		rslog.LoggerOptionsImpl{
			Output: outputDest,
			Format: rslog.JSONFormat,
			Level:  rslog.InfoLevel,
		},
		errdBuildMock,
	)
	s.NotNil(err)
}

type mockFactory struct {
	logger rslog.Logger
}

func (f *mockFactory) DefaultLogger() rslog.Logger {
	return f.logger
}

func (f *mockFactory) TerminalLogger(_ rslog.LogLevel) rslog.Logger {
	return f.logger
}

func (s *LoggerImplTestSuite) TestSetDefaultLogger() {

	lgr := rslog.DefaultLogger()

	msg := "some message"
	args := []interface{}{
		"value1",
		"value2",
	}

	loggerMock.On("Debugf", msg, args)
	rslog.Debugf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Debugf", msg, args)

	loggerMock.On("Infof", msg, args)
	rslog.Infof(msg, args...)
	loggerMock.AssertCalled(s.T(), "Infof", msg, args)

	loggerMock.On("Warnf", msg, args)
	rslog.Warnf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Warnf", msg, args)

	loggerMock.On("Errorf", msg, args)
	rslog.Errorf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Errorf", msg, args)

	loggerMock.On("Fatal", []interface{}{msg})
	rslog.Fatal(msg)
	loggerMock.AssertCalled(s.T(), "Fatal", []interface{}{msg})

	loggerMock.On("Fatalf", msg, args)
	rslog.Fatalf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Fatalf", msg, args)

	eMock := EntryMock{}
	loggerMock.On("WithField", "field", "value").Return(eMock)
	entryResultWithField := rslog.WithField("field", "value")
	s.Equal(entryResultWithField, eMock)

	f := rslog.Fields{
		"field1": "value1",
		"field2": "value2",
	}
	loggerMock.On("WithFields", f).Return(eMock)
	entryResultWithFields := rslog.WithFields(f)
	s.Equal(entryResultWithFields, eMock)

	s.Equal(rslog.DefaultLogger(), lgr)
}

func (s *LoggerImplTestSuite) TestOutputFormatUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   rslog.OutputFormat
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal TEXT OutputFormat value",
			ValueToConvert:  string(rslog.TextFormat),
			ExpectedValue:   rslog.TextFormat,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal JSON OutputFormat value",
			ValueToConvert:  string(rslog.JSONFormat),
			ExpectedValue:   rslog.JSONFormat,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal empty OutputFormat value",
			ValueToConvert:  "",
			ShouldHaveError: true,
		},
		{
			TestName:        "Unmarshal invalid OutputFormat value",
			ValueToConvert:  "anything",
			ShouldHaveError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.TestName, func() {
			var result rslog.OutputFormat
			err := result.UnmarshalText([]byte(tc.ValueToConvert))

			if tc.ShouldHaveError {
				s.NotNil(err)
			} else {
				s.Nil(err)
				s.Equal(tc.ExpectedValue, result)
			}
		})
	}
}

func (s *LoggerImplTestSuite) TestNewLoggerImplLevel() {

	cases := []struct {
		TestName               string
		LoggingLevel           rslog.LogLevel
		ExpectedLogrusLogLevel logrus.Level
	}{
		{
			TestName:               "NewLoggerImpl Trace level test",
			LoggingLevel:           rslog.TraceLevel,
			ExpectedLogrusLogLevel: logrus.TraceLevel,
		},
		{
			TestName:               "NewLoggerImpl Debug level test",
			LoggingLevel:           rslog.DebugLevel,
			ExpectedLogrusLogLevel: logrus.DebugLevel,
		},
		{
			TestName:               "NewLoggerImpl Info level test",
			LoggingLevel:           rslog.InfoLevel,
			ExpectedLogrusLogLevel: logrus.InfoLevel,
		},
		{
			TestName:               "NewLoggerImpl Warn level test",
			LoggingLevel:           rslog.WarningLevel,
			ExpectedLogrusLogLevel: logrus.WarnLevel,
		},
		{
			TestName:               "NewLoggerImpl Error level test",
			LoggingLevel:           rslog.ErrorLevel,
			ExpectedLogrusLogLevel: logrus.ErrorLevel,
		},
	}

	for _, tc := range cases {

		s.Run(tc.TestName, func() {

			outputMock := &OutputBuilderMock{}
			expectedWriter := IoWriterMock{}
			outputMock.On("Build", outputDest).Return(expectedWriter, nil)

			lgr, err := rslog.NewLoggerImpl(
				rslog.LoggerOptionsImpl{
					Output: outputDest,
					Format: rslog.TextFormat,
					Level:  tc.LoggingLevel,
				},
				outputMock,
			)
			s.Nil(err)
			s.Equal(tc.ExpectedLogrusLogLevel, lgr.CoreLogger.GetLevel())
		})
	}

}

func (s *LoggerImplTestSuite) TestLogLevelUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   rslog.LogLevel
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal Trace LogLevel value",
			ValueToConvert:  string(rslog.TraceLevel),
			ExpectedValue:   rslog.TraceLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Debug LogLevel value",
			ValueToConvert:  string(rslog.DebugLevel),
			ExpectedValue:   rslog.DebugLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Info LogLevel value",
			ValueToConvert:  string(rslog.InfoLevel),
			ExpectedValue:   rslog.InfoLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Warn LogLevel value",
			ValueToConvert:  string(rslog.WarningLevel),
			ExpectedValue:   rslog.WarningLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Error LogLevel value",
			ValueToConvert:  string(rslog.ErrorLevel),
			ExpectedValue:   rslog.ErrorLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal empty LogLevel value",
			ValueToConvert:  "",
			ShouldHaveError: true,
		},
		{
			TestName:        "Unmarshal invalid LogLevel value",
			ValueToConvert:  "anything",
			ShouldHaveError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.TestName, func() {
			var result rslog.LogLevel
			err := result.UnmarshalText([]byte(tc.ValueToConvert))

			if tc.ShouldHaveError {
				s.NotNil(err)
			} else {
				s.Nil(err)
				s.Equal(tc.ExpectedValue, result)
			}
		})
	}

}

func (s *LoggerImplTestSuite) TestOnConfigReload() {
	outputMock := &OutputBuilderMock{}
	expectedWriter := IoWriterMock{}

	outputMock.On("Build", outputDest).Return(expectedWriter, nil)

	log, err := rslog.NewLoggerImpl(
		rslog.LoggerOptionsImpl{
			Output: outputDest,
			Format: rslog.JSONFormat,
			Level:  rslog.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)

	s.Equal(log.CoreLogger.Level, logrus.InfoLevel)

	log.OnConfigReload(rslog.WarningLevel)

	s.NotNil(log.CoreLogger.Out)
	s.Equal(log.CoreLogger.Level, logrus.WarnLevel)
	s.IsType(&rslog.UTCJSONFormatter{}, log.CoreLogger.Formatter)
}

func (s *LoggerImplTestSuite) TestNewDiscardingLogger() {

	// Ensure that the discarding logger doesn't panic
	discardingLogger := rslog.NewDiscardingLogger()

	s.NotNil(discardingLogger)
}

func (s *LoggerImplTestSuite) TestUTCJSONFormatter() {

	utc, nonUtc := getTestTimes(s.Assertions)

	formatter := rslog.NewUTCJSONFormatter()

	// create a log entry with non-UTC time
	entry := &logrus.Entry{
		Level:   logrus.DebugLevel,
		Time:    nonUtc,
		Message: "test",
	}

	serialized, err := formatter.Format(entry)

	s.Nil(err)

	result := string(serialized)

	// assert that UTC time appears in the formatted entry
	utcTimestamp := utc.Format(formatter.TimestampFormat)
	s.True(strings.Contains(result, utcTimestamp))

}

func (s *LoggerImplTestSuite) TestUTCTextFormatter() {

	utc, nonUtc := getTestTimes(s.Assertions)

	formatter := rslog.NewUTCTextFormatter()

	// create a log entry with non-UTC time
	entry := &logrus.Entry{
		Level:   logrus.DebugLevel,
		Time:    nonUtc,
		Message: "test",
	}

	serialized, err := formatter.Format(entry)

	s.Nil(err)

	result := string(serialized)

	// assert that UTC time appears in the formatted entry
	utcTimestamp := utc.Format(formatter.TimestampFormat)
	s.True(strings.Contains(result, utcTimestamp))
}

// returns a tuple containing a UTC time and its conversion to a test timezone 1 hr east of UTC.
// assertions are included to prove that we have created the intended times.
func getTestTimes(a *assert.Assertions) (time.Time, time.Time) {
	// create a test location 1 hr east of UTC
	loc := time.FixedZone("test/zone", 3600)
	utc := time.Now().UTC()

	// shift UTC time to the test location
	nonUtc := utc.In(loc)

	// verify shifted time is 1 hr ahead
	expected := utc.Add(time.Hour)

	expYear, expMonth, expDay := expected.Date()
	nonUtcYear, nonUtcMonth, nonUtcDay := nonUtc.Date()

	a.Equal(expYear, nonUtcYear)
	a.Equal(expMonth, nonUtcMonth)
	a.Equal(expDay, nonUtcDay)

	expHour, expMin, expSec := expected.Clock()
	nonUtcHour, nonUtcMin, nonUtcSec := nonUtc.Clock()

	a.Equal(expHour, nonUtcHour)
	a.Equal(expMin, nonUtcMin)
	a.Equal(expSec, nonUtcSec)

	// verify shifted time is not in UTC location
	a.NotEqual(utc.Location(), nonUtc.Location())

	return utc, nonUtc
}
