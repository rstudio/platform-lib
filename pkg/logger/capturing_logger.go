package logger

// Copyright (C) 2021 by RStudio, PBC.

import "fmt"

// CapturingLogger is a direct logger that remembers _all_ its logged
// messages. Useful for tests.
type CapturingLogger struct {
	directLogger
	Messages []string
}

func (logger *CapturingLogger) Logf(msg string, args ...interface{}) {
	logger.Messages = append(logger.Messages,
		fmt.Sprintf(msg, args...),
	)
	logger.directLogger.Logf(msg, args...)
}

func (logger *CapturingLogger) Output(msg string) {
	logger.Messages = append(logger.Messages, msg)
}

func (logger *CapturingLogger) Reset() {
	logger.Messages = nil
}

// CaptureOnlyLogger is a logger that remembers its logged
// messages, but doesn't log them anywhere else.
type CaptureOnlyLogger struct {
	Messages []string
}

func (logger *CaptureOnlyLogger) Logf(msg string, args ...interface{}) {
	logger.Output(fmt.Sprintf(msg, args...))
}

func (logger *CaptureOnlyLogger) Output(msg string) {
	logger.Messages = append(logger.Messages, msg)
}

func (logger *CaptureOnlyLogger) Reset() {
	logger.Messages = nil
}
