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

type TestLogger struct {
	enabled bool
}

func (l *TestLogger) Debugf(msg string, args ...interface{}) {
	log.Printf(msg, args...)
}

func (l *TestLogger) Enabled() bool {
	return l.enabled
}

type TestIPReporter struct {
	Ip string
}

func (l *TestIPReporter) IP() string {
	return l.Ip
}
