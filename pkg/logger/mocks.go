package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"

	"github.com/stretchr/testify/mock"
)

type outputBuilderMock struct {
	mock.Mock
}

func (m *outputBuilderMock) Build(outputType LogOutputType, path string) io.Writer {
	args := m.Called(outputType, path)
	return args.Get(0).(io.Writer)
}

type ioWriterMock struct {
	io.Writer
}

type entryMock struct {
	Entry
}

// Useful to mock and test logging calls
type RSCLoggerMock struct {
	mock.Mock

	stringCalls []string
}

// Register expected methods (Infof, Warnf, Errorf...)
// allowing to be called with any message and arguments
func (m *RSCLoggerMock) AllowAny(methods ...string) {
	for _, method := range methods {
		m.On(method, mock.AnythingOfType("string"), mock.Anything)
	}
}

// Remove all calls history
func (m *RSCLoggerMock) Clear() {
	m.stringCalls = make([]string, 0)
	m.Calls = make([]mock.Call, 0)
}

// Get logging call result message by index
func (m *RSCLoggerMock) Call(index int) string {
	return m.stringCalls[index]
}

// Get the last logging call result message
func (m *RSCLoggerMock) LastCall() string {
	calls := m.stringCalls
	return calls[len(calls)-1]
}

func (m *RSCLoggerMock) Debugf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *RSCLoggerMock) Info(msg string) {
	m.stringCalls = append(m.stringCalls, msg)
	m.Called(msg, []interface{}(nil))
}

func (m *RSCLoggerMock) Infof(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *RSCLoggerMock) Warnf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *RSCLoggerMock) Errorf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *RSCLoggerMock) Fatal(args ...interface{}) {
	m.Called(args)
}

func (m *RSCLoggerMock) Fatalf(msg string, args ...interface{}) {
	m.stringCalls = append(m.stringCalls, fmt.Sprintf(msg, args...))
	m.Called(msg, args)
}

func (m *RSCLoggerMock) WithField(key string, value interface{}) Entry {
	args := m.Called(key, value)
	return args.Get(0).(Entry)
}

func (m *RSCLoggerMock) WithFields(fields Fields) Entry {
	args := m.Called(fields)
	return args.Get(0).(Entry)
}

func (m *RSCLoggerMock) WithCorrelationID(correlationID string) Entry {
	args := m.Called(correlationID)
	return args.Get(0).(Entry)
}

func (m *RSCLoggerMock) Copy() RSCLogger {
	args := m.Called()
	return args.Get(0).(RSCLogger)
}

func (m *RSCLoggerMock) SetLevel(level LogLevel) {
	m.Called(level)
}

func (m *RSCLoggerMock) SetOutput(output io.Writer) {
	m.Called(output)
}

func (m *RSCLoggerMock) OnConfigReload(level LogLevel) {
	m.Called(level)
}

func (m *RSCLoggerMock) SetReportCaller(flag bool) {
	m.Called(flag)
}
