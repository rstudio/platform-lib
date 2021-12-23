package rslog

// Copyright (C) 2021 by RStudio, PBC.

// A logger that directly sends to `log`
type directLogger struct{}

func (logger directLogger) Logf(msg string, args ...interface{}) {
	_log_printf(msg, args...)
}

var DirectLogger directLogger = directLogger{}
