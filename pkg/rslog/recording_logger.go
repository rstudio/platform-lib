package logger

// Copyright (C) 2021 by RStudio, PBC.

import "fmt"

// RecordingLogger is a direct logger that remembers its last Logf call and
// its rendered result. Useful for tests.
type RecordingLogger struct {
	directLogger
	Called  bool
	Format  string
	Args    []interface{}
	Message string
}

// Reset clears any previously recorded state.
func (logger *RecordingLogger) Reset() {
	logger.Called = false
	logger.Format = ""
	logger.Args = nil
	logger.Message = ""
}

func (logger *RecordingLogger) Logf(msg string, args ...interface{}) {
	logger.Called = true
	logger.Format = msg
	logger.Args = args
	logger.Message = fmt.Sprintf(msg, args...)
	logger.directLogger.Logf(msg, args...)
}
