package loggertest

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"

	"github.com/rstudio/platform-lib/pkg/logger"
	"github.com/stretchr/testify/mock"
)

type OutputBuilderMock struct {
	mock.Mock
}

func (m *OutputBuilderMock) Build(output logger.LogOutputType, logFilePath, defaultLogFilePath string) io.Writer {
	args := m.Called(output, logFilePath, defaultLogFilePath)
	return args.Get(0).(io.Writer)
}

type IoWriterMock struct {
	io.Writer
}

type EntryMock struct {
	logger.Logger
}

// Useful to mock and test logging calls
type LoggerMock struct {
	mock.Mock

	stringCalls []string
}

// Register expected methods (Infof, Warnf, Errorf...)
// allowing to be called with any message and arguments
func (m *LoggerMock) AllowAny(methods ...string) {
	for _, method := range methods {
		m.On(method, mock.AnythingOfType("string"), mock.Anything)
	}
}

// Remove all calls history
func (m *LoggerMock) Clear() {
	m.stringCalls = make([]string, 0)
	m.Calls = make([]mock.Call, 0)
}

// Get logging call result message by index
func (m *LoggerMock) Call(index int) string {
	return m.stringCalls[index]
}

// Get the last logging call result message
func (m *LoggerMock) LastCall() string {
	calls := m.stringCalls
	return calls[len(calls)-1]
}

func (m *LoggerMock) Debugf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *LoggerMock) Info(msg string) {
	m.stringCalls = append(m.stringCalls, msg)
	m.Called(msg, []interface{}(nil))
}

func (m *LoggerMock) Infof(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *LoggerMock) Warnf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *LoggerMock) Errorf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *LoggerMock) Fatal(args ...interface{}) {
	m.Called(args)
}

func (m *LoggerMock) Fatalf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *LoggerMock) WithField(key string, value interface{}) logger.Logger {
	args := m.Called(key, value)
	return args.Get(0).(logger.Logger)
}

func (m *LoggerMock) WithFields(fields logger.Fields) logger.Logger {
	args := m.Called(fields)
	return args.Get(0).(logger.Logger)
}

func (m *LoggerMock) WithCorrelationID(correlationID string) logger.Logger {
	args := m.Called(correlationID)
	return args.Get(0).(logger.Logger)
}

func (m *LoggerMock) Copy() logger.Logger {
	args := m.Called()
	return args.Get(0).(logger.Logger)
}

func (m *LoggerMock) SetLevel(level logger.LogLevel) {
	m.Called(level)
}

func (m *LoggerMock) SetOutput(writers ...io.Writer) {
	m.Called(writers)
}

func (m *LoggerMock) OnConfigReload(level logger.LogLevel) {
	m.Called(level)
}

func (m *LoggerMock) SetReportCaller(flag bool) {
	m.Called(flag)
}

type DebugLoggerMock struct {
	LoggerMock
}

func (m *DebugLoggerMock) Enabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *DebugLoggerMock) WithSubRegion(subregion string) *DebugLoggerMock {
	args := m.Called(subregion)
	return args.Get(0).(*DebugLoggerMock)
}
