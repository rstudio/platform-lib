package rslogtest

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"fmt"
	"io"
	"log"
	"regexp"
	"sync"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/stretchr/testify/mock"
)

type OutputBuilderMock struct {
	mock.Mock
}

func (m *OutputBuilderMock) Build(o ...rslog.OutputDest) (io.Writer, error) {
	args := m.Called(o)
	return args.Get(0).(io.Writer), args.Error(1)
}

type IoWriterMock struct {
	io.Writer
}

type EntryMock struct {
	rslog.Logger
}

// Useful to mock and test logging calls
type LoggerMock struct {
	mock.Mock

	// mutex guards stringCalls.
	mutex sync.Mutex

	// stringCalls stores formatted messages.
	stringCalls []string
}

// Register expected methods (Infof, Warnf, Errorf...)
// allowing to be called with any message and arguments
func (m *LoggerMock) AllowAny(methods ...string) {
	for _, method := range methods {
		if method == "Fatal" {
			m.On(method, mock.Anything)
		} else {
			m.On(method, mock.AnythingOfType("string"), mock.Anything)
		}
	}
}

// Remove all calls history
func (m *LoggerMock) Clear() {
	m.mutex.Lock()
	m.stringCalls = make([]string, 0)
	m.mutex.Unlock()
	// Mock.Calls is not concurrency-safe.
	// See: https://github.com/stretchr/testify/issues/1128
	m.Mock.Calls = make([]mock.Call, 0)
}

// Get logging call result message by index
func (m *LoggerMock) Call(index int) string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.stringCalls[index]
}

// Get the last logging call result message
func (m *LoggerMock) LastCall() string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	calls := m.stringCalls
	return calls[len(calls)-1]
}

// Determine if provided messages match with calls with no particular order
func (m *LoggerMock) MessagesMatch(matchRgx []string) bool {
	for _, rgx := range matchRgx {
		entryResult := false
		rxObj, err := regexp.Compile("^" + rgx + "$")
		if err != nil {
			log.Output(2, fmt.Sprintf("LoggerMock.MessagesMatch: Could not compile regex: %s", rgx))
			return false
		}

		for i := range m.Calls {
			msg := m.Call(i)
			if entryResult = rxObj.MatchString(msg); entryResult {
				break
			}
		}

		if !entryResult {
			log.Output(2, fmt.Sprintf("%s did not match any message", rgx))
			return false
		}
	}
	return true
}

// recordCallFormatted formats and retains the message.
//
// Available through Call(int) and LastCall().
func (m *LoggerMock) recordCallFormatted(msg string, args ...interface{}) {
	// keep the critical section by formatting outside.
	formatted := fmt.Sprintf(msg, args...)

	m.mutex.Lock()
	m.stringCalls = append(m.stringCalls, formatted)
	m.mutex.Unlock()
}

// recordCallSimple retains a message that does not require formatting.
//
// Available through Call(int) and LastCall().
func (m *LoggerMock) recordCallSimple(parts ...interface{}) {
	// keep the critical section small by formatting outside.
	simple := fmt.Sprint(parts...)

	m.mutex.Lock()
	m.stringCalls = append(m.stringCalls, simple)
	m.mutex.Unlock()
}

func (m *LoggerMock) Debugf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Tracef(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Info(msg string) {
	m.recordCallSimple(msg)
	m.Called(msg, []interface{}(nil))
}

func (m *LoggerMock) Infof(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Warnf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Errorf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Fatal(args ...interface{}) {
	m.recordCallSimple(args...)
	m.Called(args)
}

func (m *LoggerMock) Fatalf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

// TODO: remove this function when the Connect migration process to the new logging standard is complete.
func (m *LoggerMock) Logf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) Panicf(msg string, args ...interface{}) {
	m.recordCallFormatted(msg, args...)
	m.Called(msg, args)
}

func (m *LoggerMock) WithField(key string, value interface{}) rslog.Logger {
	args := m.Called(key, value)
	return args.Get(0).(rslog.Logger)
}

func (m *LoggerMock) WithFields(fields rslog.Fields) rslog.Logger {
	args := m.Called(fields)
	return args.Get(0).(rslog.Logger)
}

func (m *LoggerMock) WithCorrelationID(correlationID string) rslog.Logger {
	args := m.Called(correlationID)
	return args.Get(0).(rslog.Logger)
}

func (m *LoggerMock) SetLevel(level rslog.LogLevel) {
	m.Called(level)
}

func (m *LoggerMock) SetFormatter(format rslog.OutputFormat) {
	m.Called(format)
}

func (m *LoggerMock) SetOutput(writers ...io.Writer) {
	m.Called(writers)
}

func (m *LoggerMock) OnConfigReload(level rslog.LogLevel) {
	m.Called(level)
}

type DebugLoggerMock struct {
	LoggerMock
}

func (m *DebugLoggerMock) Enabled() bool {
	args := m.Called()
	return args.Get(0).(bool)
}

func (m *DebugLoggerMock) WithFields(fields rslog.Fields) rslog.DebugLogger {
	args := m.Called(fields)
	return args.Get(0).(rslog.DebugLogger)
}

func (m *DebugLoggerMock) WithSubRegion(subregion string) rslog.DebugLogger {
	args := m.Called(subregion)
	return args.Get(0).(rslog.DebugLogger)
}
