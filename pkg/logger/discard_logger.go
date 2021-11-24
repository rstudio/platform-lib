package logger

// Copyright (C) 2021 by RStudio, PBC.

import "io"

// A logger that drops all messages. Useful for tests.
type discardLogger struct{}

func (discardLogger) Logf(msg string, args ...interface{})   {}
func (discardLogger) Debugf(msg string, args ...interface{}) {}
func (discardLogger) Infof(msg string, args ...interface{})  {}
func (discardLogger) Warnf(msg string, args ...interface{})  {}
func (discardLogger) Errorf(msg string, args ...interface{}) {}
func (discardLogger) Fatalf(msg string, args ...interface{}) {}
func (discardLogger) Fatal(args ...interface{})              {}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithField(key string, value interface{}) Logger {
	return l
}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithFields(fields Fields) Logger {
	return l
}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithCorrelationID(correlationID string) Logger {
	return l
}

func (l discardLogger) Copy() Logger {
	return l
}

func (discardLogger) SetLevel(level LogLevel)       {}
func (discardLogger) SetOutput(output io.Writer)    {}
func (discardLogger) OnConfigReload(level LogLevel) {}
func (discardLogger) SetReportCaller(flag bool)     {}

var DiscardLogger discardLogger = discardLogger{}

type discardOutputter struct{}

func (discardOutputter) Output(msg string) {}

// An output logger that drops all messages. Useful for tests.
type discardOutputLogger struct {
	discardLogger
	discardOutputter
}
