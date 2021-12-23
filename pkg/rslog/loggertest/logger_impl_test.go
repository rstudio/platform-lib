package loggertest

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
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

var outputDest = []logger.OutputDest{
	{
		Output:      logger.LogOutputFile,
		Filepath:    "/custom/dir/server.log",
		DefaultFile: "/var/log/rstudio/rstudio-xyz/rstudio-xyz.log",
	},
}

func (s *LoggerImplTestSuite) TestNewLoggerImpl() {
	outputMock := &OutputBuilderMock{}
	outputMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(IoWriterMock{}, nil)

	result, err := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.TextFormat,
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.NotNil(result.Out)
	s.IsType(&logrus.TextFormatter{}, result.Formatter)

	result, err = logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.JSONFormat,
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.IsType(&logrus.JSONFormatter{}, result.Formatter)

	result, err = logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.OutputFormat("UnsupportedFormat"),
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.IsType(&logrus.TextFormatter{}, result.Formatter)

	errdBuildMock := &OutputBuilderMock{}
	errdBuildMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(IoWriterMock{}, fmt.Errorf("output build error"))
	result, err = logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.JSONFormat,
			Level:  logger.InfoLevel,
		},
		errdBuildMock,
	)
	s.NotNil(err)
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

func (s *LoggerImplTestSuite) TestMultipleOutput() {
	outputMock := &OutputBuilderMock{}
	outputMock.On("Build", logger.LogOutputStdout, "").Return(IoWriterMock{}, nil)
	outputMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(IoWriterMock{}, nil)

	multiOutput := []logger.OutputDest{
		{
			Output:      logger.LogOutputFile,
			Filepath:    "/custom/dir/server.log",
			DefaultFile: "/var/log/rstudio/rstudio-xyz/rstudio-xyz.log",
		},
		{
			Output: logger.LogOutputStdout,
		},
	}

	result, err := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: multiOutput,
			Format: logger.TextFormat,
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)
	s.NotNil(result.Out)
	outputMock.AssertExpectations(s.T())
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

func (s *LoggerImplTestSuite) TestNewLoggerImplLevel() {

	cases := []struct {
		TestName               string
		LoggingLevel           logger.LogLevel
		ExpectedLogrusLogLevel logrus.Level
	}{
		{
			TestName:               "NewLoggerImpl Trace level test",
			LoggingLevel:           logger.TraceLevel,
			ExpectedLogrusLogLevel: logrus.TraceLevel,
		},
		{
			TestName:               "NewLoggerImpl Debug level test",
			LoggingLevel:           logger.DebugLevel,
			ExpectedLogrusLogLevel: logrus.DebugLevel,
		},
		{
			TestName:               "NewLoggerImpl Info level test",
			LoggingLevel:           logger.InfoLevel,
			ExpectedLogrusLogLevel: logrus.InfoLevel,
		},
		{
			TestName:               "NewLoggerImpl Warn level test",
			LoggingLevel:           logger.WarningLevel,
			ExpectedLogrusLogLevel: logrus.WarnLevel,
		},
		{
			TestName:               "NewLoggerImpl Error level test",
			LoggingLevel:           logger.ErrorLevel,
			ExpectedLogrusLogLevel: logrus.ErrorLevel,
		},
	}

	for _, tc := range cases {

		s.Run(tc.TestName, func() {

			outputMock := &OutputBuilderMock{}
			expectedWriter := IoWriterMock{}
			outputMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(expectedWriter, nil)

			lgr, err := logger.NewLoggerImpl(
				logger.LoggerOptionsImpl{
					Output: outputDest,
					Format: logger.TextFormat,
					Level:  tc.LoggingLevel,
				},
				outputMock,
			)
			s.Nil(err)

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

	outputMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(expectedWriter, nil)

	log, err := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.JSONFormat,
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)

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

	outputMock.On("Build", logger.LogOutputFile, "/custom/dir/server.log").Return(expectedWriter, nil)

	log, err := logger.NewLoggerImpl(
		logger.LoggerOptionsImpl{
			Output: outputDest,
			Format: logger.JSONFormat,
			Level:  logger.InfoLevel,
		},
		outputMock,
	)
	s.Nil(err)

	s.Equal(log.Level, logrus.InfoLevel)

	log.OnConfigReload(logger.WarningLevel)

	s.NotNil(log.Out)
	s.Equal(log.Level, logrus.WarnLevel)
	s.IsType(&logrus.JSONFormatter{}, log.Formatter)
}
