package loggertest

// Copyright (C) 2021 by RStudio, PBC.

import (
	"os"
	"testing"

	"github.com/rstudio/platform-lib/pkg/logger"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type LoggerImplTestSuite struct {
	suite.Suite
}

func TestLoggerImplSuite(t *testing.T) {
	suite.Run(t, &LoggerImplTestSuite{})
}

func (s *LoggerImplTestSuite) TestNewRSCLoggerNewLoggingEnabled() {
	outputMock := &OutputBuilderMock{}

	expectedWriter := IoWriterMock{}
	outputMock.On("Build", logger.LogOutputFile, "/path", "/path").Return(expectedWriter)

	result := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Enabled:  true,
			Output:   logger.LogOutputFile,
			Format:   logger.TextFormat,
			Level:    logger.InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)
	s.Equal(result.Out, expectedWriter)
	s.IsType(&logrus.TextFormatter{}, result.Formatter)

	result = logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Enabled:  true,
			Output:   logger.LogOutputFile,
			Format:   logger.JSONFormat,
			Level:    logger.InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)
	s.IsType(&logrus.JSONFormatter{}, result.Formatter)

	result = logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Enabled:  true,
			Output:   logger.LogOutputFile,
			Format:   logger.OutputFormat("UnsupportedFormat"),
			Level:    logger.InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	s.IsType(&logrus.TextFormatter{}, result.Formatter)
}

func (s *LoggerImplTestSuite) TestSetDefaultLogger() {
	lgr := logger.DefaultLogger()
	loggerMock := &LoggerMock{}

	logger.SetDefaultLogger(loggerMock)

	s.Equal(logger.DefaultLogger(), loggerMock)

	msg := "some message"
	args := []interface{}{
		"value1",
		"value2",
	}

	loggerMock.On("Debugf", msg, args)
	logger.Debugf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Debugf", msg, args)

	loggerMock.On("Infof", msg, args)
	logger.Infof(msg, args...)
	loggerMock.AssertCalled(s.T(), "Infof", msg, args)

	loggerMock.On("Warnf", msg, args)
	logger.Warnf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Warnf", msg, args)

	loggerMock.On("Errorf", msg, args)
	logger.Errorf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Errorf", msg, args)

	loggerMock.On("Fatal", []interface{}{msg})
	logger.Fatal(msg)
	loggerMock.AssertCalled(s.T(), "Fatal", []interface{}{msg})

	loggerMock.On("Fatalf", msg, args)
	logger.Fatalf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Fatalf", msg, args)

	eMock := EntryMock{}
	loggerMock.On("WithField", "field", "value").Return(eMock)
	entryResultWithField := logger.WithField("field", "value")
	s.Equal(entryResultWithField, eMock)

	f := logger.Fields{
		"field1": "value1",
		"field2": "value2",
	}
	loggerMock.On("WithFields", f).Return(eMock)
	entryResultWithFields := logger.WithFields(f)
	s.Equal(entryResultWithFields, eMock)

	logger.SetDefaultLogger(lgr)
	s.Equal(logger.DefaultLogger(), lgr)
}

func (s *LoggerImplTestSuite) TestOutputFormatUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   logger.OutputFormat
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal TEXT OutputFormat value",
			ValueToConvert:  string(logger.TextFormat),
			ExpectedValue:   logger.TextFormat,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal JSON OutputFormat value",
			ValueToConvert:  string(logger.JSONFormat),
			ExpectedValue:   logger.JSONFormat,
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
			var result logger.OutputFormat
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

func (s *LoggerImplTestSuite) TestNewRSCLoggerLevel() {

	cases := []struct {
		TestName               string
		LoggingLevel           logger.LogLevel
		ExpectedLogrusLogLevel logrus.Level
	}{
		{
			TestName:               "NewRSCLogger Trace level test",
			LoggingLevel:           logger.TraceLevel,
			ExpectedLogrusLogLevel: logrus.TraceLevel,
		},
		{
			TestName:               "NewRSCLogger Debug level test",
			LoggingLevel:           logger.DebugLevel,
			ExpectedLogrusLogLevel: logrus.DebugLevel,
		},
		{
			TestName:               "NewRSCLogger Info level test",
			LoggingLevel:           logger.InfoLevel,
			ExpectedLogrusLogLevel: logrus.InfoLevel,
		},
		{
			TestName:               "NewRSCLogger Warn level test",
			LoggingLevel:           logger.WarningLevel,
			ExpectedLogrusLogLevel: logrus.WarnLevel,
		},
		{
			TestName:               "NewRSCLogger Error level test",
			LoggingLevel:           logger.ErrorLevel,
			ExpectedLogrusLogLevel: logrus.ErrorLevel,
		},
	}

	for _, tc := range cases {

		s.Run(tc.TestName, func() {

			outputMock := &OutputBuilderMock{}
			expectedWriter := IoWriterMock{}
			outputMock.On("Build", logger.LogOutputFile, "/path", "/path").Return(expectedWriter)

			lgr := logger.NewLoggerImpl(
				logger.LoggerOptionsImpl{
					Enabled:  true,
					Output:   logger.LogOutputFile,
					Format:   logger.TextFormat,
					Level:    tc.LoggingLevel,
					Filepath: "/path",
				},
				outputMock,
			)

			logrusLogger := lgr.Logger

			s.Equal(tc.ExpectedLogrusLogLevel, logrusLogger.GetLevel())
		})
	}

}

func (s *LoggerImplTestSuite) TestLogLevelUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   logger.LogLevel
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal Trace LogLevel value",
			ValueToConvert:  string(logger.TraceLevel),
			ExpectedValue:   logger.TraceLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Debug LogLevel value",
			ValueToConvert:  string(logger.DebugLevel),
			ExpectedValue:   logger.DebugLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Info LogLevel value",
			ValueToConvert:  string(logger.InfoLevel),
			ExpectedValue:   logger.InfoLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Warn LogLevel value",
			ValueToConvert:  string(logger.WarningLevel),
			ExpectedValue:   logger.WarningLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Error LogLevel value",
			ValueToConvert:  string(logger.ErrorLevel),
			ExpectedValue:   logger.ErrorLevel,
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
			var result logger.LogLevel
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

func (s *LoggerImplTestSuite) TestCopy() {

	outputMock := &OutputBuilderMock{}
	expectedWriter := IoWriterMock{}

	outputMock.On("Build", logger.LogOutputFile, "/path", "/path").Return(expectedWriter)

	log := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Enabled:  true,
			Output:   logger.LogOutputFile,
			Format:   logger.JSONFormat,
			Level:    logger.InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	copy := log.Copy()

	s.Equal(log.Out, copy.(logger.LoggerImpl).Out)
	s.Equal(log.Level, copy.(logger.LoggerImpl).Level)
	s.Equal(log.Formatter, copy.(logger.LoggerImpl).Formatter)

	log.Logger.SetLevel(logrus.DebugLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	log.SetOutput(os.Stdout)

	s.NotEqual(log.Level, copy.(logger.LoggerImpl).Level)
	s.NotEqual(log.Formatter, copy.(logger.LoggerImpl).Formatter)
	s.NotEqual(log.Out, copy.(logger.LoggerImpl).Out)
}

func (s *LoggerImplTestSuite) TestOnConfigReload() {
	outputMock := &OutputBuilderMock{}
	expectedWriter := IoWriterMock{}

	outputMock.On("Build", logger.LogOutputFile, "/path", "/path").Return(expectedWriter)

	log := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Enabled:  true,
			Output:   logger.LogOutputFile,
			Format:   logger.JSONFormat,
			Level:    logger.InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	s.Equal(log.Level, logrus.InfoLevel)

	log.OnConfigReload(logger.WarningLevel)

	s.Equal(log.Out, expectedWriter)
	s.Equal(log.Level, logrus.WarnLevel)
	s.IsType(&logrus.JSONFormatter{}, log.Formatter)
}
