package listener

// Copyright (C) 2022 by RStudio, PBC.

import "log"

type TestNotification struct {
	Val     string
	GuidVal string
}

func (t *TestNotification) Type() uint8 {
	return 1
}

func (t *TestNotification) Guid() string {
	return t.GuidVal
}

func (t *TestNotification) Data() interface{} {
	return t.Val
}

type TestLogger struct {
	enabled bool
}

func (l *TestLogger) Debugf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (l *TestLogger) Enabled() bool {
	return l.enabled
}
