package rslog

// Copyright (C) 2021 by RStudio, PBC.

import "fmt"

// CapturingPassthroughLogger is a logger that remembers _all_ its logged
// messages and passes them through to a parent logger.
type CapturingPassthroughLogger struct {
	parent   DeprecatedLogger
	Messages []string
}

func NewCapturingPassthroughLogger(parent DeprecatedLogger) *CapturingPassthroughLogger {
	return &CapturingPassthroughLogger{
		parent: parent,
	}
}

func (logger *CapturingPassthroughLogger) Logf(msg string, args ...interface{}) {
	logger.Messages = append(logger.Messages,
		fmt.Sprintf(msg, args...),
	)
	logger.parent.Logf(msg, args...)
}

func (logger *CapturingPassthroughLogger) Output(msg string) {
	logger.Messages = append(logger.Messages, msg)
	logger.parent.Logf("%s", msg)
}

func (logger *CapturingPassthroughLogger) Reset() {
	logger.Messages = nil
}
