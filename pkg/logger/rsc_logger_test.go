package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"bytes"
	"log"
	"os"

	"github.com/sirupsen/logrus"
)

func (s *LoggerSuite) TestNewRSCLoggerNewLoggingEnabled() {
	outputMock := &outputBuilderMock{}

	expectedWriter := ioWriterMock{}
	outputMock.On("Build", LogOutputFile, "/path").Return(expectedWriter)

	result := NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  true,
			Output:   LogOutputFile,
			Format:   TextFormat,
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	s.IsType(&logrusImpl{}, result)
	logger := result.(*logrusImpl)

	s.Equal(logger.Out, expectedWriter)
	s.IsType(&logrus.TextFormatter{}, logger.Formatter)

	result = NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  true,
			Output:   LogOutputFile,
			Format:   JSONFormat,
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	logger = result.(*logrusImpl)
	s.IsType(&logrus.JSONFormatter{}, logger.Formatter)

	result = NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  true,
			Output:   LogOutputFile,
			Format:   OutputFormat("UnsupportedFormat"),
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	logger = result.(*logrusImpl)
	s.IsType(&logrus.TextFormatter{}, logger.Formatter)
}

func (s *LoggerSuite) TestNewRSCLoggerNewLoggingDisabled() {
	outputMock := &outputBuilderMock{}

	result := NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  false,
			Output:   LogOutputFile,
			Format:   TextFormat,
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	s.IsType(&legacyLogger{}, result)
}

func (s *LoggerSuite) TestSetDefaultLogger() {
	logger := defaultLogger
	loggerMock := &RSCLoggerMock{}

	SetDefaultLogger(loggerMock)

	s.Equal(defaultLogger, loggerMock)

	msg := "some message"
	args := []interface{}{
		"value1",
		"value2",
	}

	loggerMock.On("Debugf", msg, args)
	Debugf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Debugf", msg, args)

	loggerMock.On("Infof", msg, args)
	Infof(msg, args...)
	loggerMock.AssertCalled(s.T(), "Infof", msg, args)

	loggerMock.On("Warnf", msg, args)
	Warnf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Warnf", msg, args)

	loggerMock.On("Errorf", msg, args)
	Errorf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Errorf", msg, args)

	loggerMock.On("Fatal", []interface{}{msg})
	Fatal(msg)
	loggerMock.AssertCalled(s.T(), "Fatal", []interface{}{msg})

	loggerMock.On("Fatalf", msg, args)
	Fatalf(msg, args...)
	loggerMock.AssertCalled(s.T(), "Fatalf", msg, args)

	eMock := entryMock{}
	loggerMock.On("WithField", "field", "value").Return(eMock)
	entryResultWithField := WithField("field", "value")
	s.Equal(entryResultWithField, eMock)

	f := Fields{
		"field1": "value1",
		"field2": "value2",
	}
	loggerMock.On("WithFields", f).Return(eMock)
	entryResultWithFields := WithFields(f)
	s.Equal(entryResultWithFields, eMock)

	correlation_Id := "correlation_Id"
	loggerMock.On("WithField", correlationIDKey, correlation_Id).Return(eMock)
	entryResultWithCorrelationID := WithCorrelationID(correlation_Id)
	s.Equal(entryResultWithCorrelationID, eMock)

	SetDefaultLogger(logger)
	s.Equal(defaultLogger, logger)
}

func (s *LoggerSuite) TestOutputFormatUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   OutputFormat
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal TEXT OutputFormat value",
			ValueToConvert:  string(TextFormat),
			ExpectedValue:   TextFormat,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal JSON OutputFormat value",
			ValueToConvert:  string(JSONFormat),
			ExpectedValue:   JSONFormat,
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
			var result OutputFormat
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

func (s *LoggerSuite) TestNewRSCLoggerLevel() {

	cases := []struct {
		TestName               string
		LoggingLevel           LogLevel
		ExpectedLogrusLogLevel logrus.Level
	}{
		{
			TestName:               "NewRSCLogger Trace level test",
			LoggingLevel:           TraceLevel,
			ExpectedLogrusLogLevel: logrus.TraceLevel,
		},
		{
			TestName:               "NewRSCLogger Debug level test",
			LoggingLevel:           DebugLevel,
			ExpectedLogrusLogLevel: logrus.DebugLevel,
		},
		{
			TestName:               "NewRSCLogger Info level test",
			LoggingLevel:           InfoLevel,
			ExpectedLogrusLogLevel: logrus.InfoLevel,
		},
		{
			TestName:               "NewRSCLogger Warn level test",
			LoggingLevel:           WarningLevel,
			ExpectedLogrusLogLevel: logrus.WarnLevel,
		},
		{
			TestName:               "NewRSCLogger Error level test",
			LoggingLevel:           ErrorLevel,
			ExpectedLogrusLogLevel: logrus.ErrorLevel,
		},
	}

	for _, tc := range cases {

		s.Run(tc.TestName, func() {

			outputMock := &outputBuilderMock{}
			expectedWriter := ioWriterMock{}
			outputMock.On("Build", LogOutputFile, "/path").Return(expectedWriter)

			logger := NewRSCLogger(
				RSCLoggerOptions{
					Enabled:  true,
					Output:   LogOutputFile,
					Format:   TextFormat,
					Level:    tc.LoggingLevel,
					Filepath: "/path",
				},
				outputMock,
			)

			logrusLogger := logger.(*logrusImpl)

			s.Equal(tc.ExpectedLogrusLogLevel, logrusLogger.GetLevel())
		})
	}

}

func (s *LoggerSuite) TestLogLevelUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   LogLevel
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal Trace LogLevel value",
			ValueToConvert:  string(TraceLevel),
			ExpectedValue:   TraceLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Debug LogLevel value",
			ValueToConvert:  string(DebugLevel),
			ExpectedValue:   DebugLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Info LogLevel value",
			ValueToConvert:  string(InfoLevel),
			ExpectedValue:   InfoLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Warn LogLevel value",
			ValueToConvert:  string(WarningLevel),
			ExpectedValue:   WarningLevel,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal Error LogLevel value",
			ValueToConvert:  string(ErrorLevel),
			ExpectedValue:   ErrorLevel,
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
			var result LogLevel
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

func (s *LoggerSuite) TestCopy() {

	outputMock := &outputBuilderMock{}
	expectedWriter := ioWriterMock{}

	outputMock.On("Build", LogOutputFile, "/path").Return(expectedWriter)

	log := NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  true,
			Output:   LogOutputFile,
			Format:   JSONFormat,
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	copy := log.Copy()

	s.Equal(log.(*logrusImpl).Out, copy.(logrusImpl).Out)
	s.Equal(log.(*logrusImpl).Level, copy.(logrusImpl).Level)
	s.Equal(log.(*logrusImpl).Formatter, copy.(logrusImpl).Formatter)

	log.(*logrusImpl).Logger.SetLevel(logrus.DebugLevel)
	log.(*logrusImpl).SetFormatter(&logrus.TextFormatter{})
	log.(*logrusImpl).SetOutput(os.Stdout)

	s.NotEqual(log.(*logrusImpl).Level, copy.(logrusImpl).Level)
	s.NotEqual(log.(*logrusImpl).Formatter, copy.(logrusImpl).Formatter)
	s.NotEqual(log.(*logrusImpl).Out, copy.(logrusImpl).Out)
}

func (s *LoggerSuite) TestOnConfigReload() {
	outputMock := &outputBuilderMock{}
	expectedWriter := ioWriterMock{}

	outputMock.On("Build", LogOutputFile, "/path").Return(expectedWriter)

	log := NewRSCLogger(
		RSCLoggerOptions{
			Enabled:  true,
			Output:   LogOutputFile,
			Format:   JSONFormat,
			Level:    InfoLevel,
			Filepath: "/path",
		},
		outputMock,
	)

	s.Equal(log.(*logrusImpl).Level, logrus.InfoLevel)

	log.OnConfigReload(WarningLevel)

	s.Equal(log.(*logrusImpl).Out, expectedWriter)
	s.Equal(log.(*logrusImpl).Level, logrus.WarnLevel)
	s.IsType(&logrus.JSONFormatter{}, log.(*logrusImpl).Formatter)
}

func (s *LoggerSuite) TestLegacyLoggerWithPreamble() {
	var testBuf bytes.Buffer
	log.SetOutput(&testBuf)

	lgr := NewLegacyLogger()
	lgr.Infof("with preamble: %s", "no")
	s.Contains(testBuf.String(), "with preamble: no")

	testBuf.Reset()
	preambleLgr := lgr.WithField("a_key", "updating sensors")
	preambleLgr.Infof("with preamble: %s", "yes")
	s.Contains(testBuf.String(), "[a_key: updating sensors] with preamble: yes")

	// Extend with more key:val
	testBuf.Reset()
	extendedPreambleLgr := preambleLgr.WithField("extra_key", "fox jumping through snow")
	extendedPreambleLgr.Infof("with preamble: %s", "yes")
	s.Contains(testBuf.String(), "[a_key: updating sensors; extra_key: fox jumping through snow] with preamble: yes")

	// Original preamble should have the same and only key:val
	testBuf.Reset()
	preambleLgr.Infof("with preamble: %s", "yes")
	s.Contains(testBuf.String(), "[a_key: updating sensors] with preamble: yes")

	testBuf.Reset()
	preambleLgr = lgr.WithFields(Fields{
		"operation": "1000",
		"status":    "killed",
	})
	preambleLgr.Infof("with preamble: %s", "yes")
	s.Contains(testBuf.String(), "[operation: 1000; status: killed] with preamble: yes")
}
