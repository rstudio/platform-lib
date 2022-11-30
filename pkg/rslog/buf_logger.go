package rslog

// Copyright (C) 2022 by RStudio, PBC.

import "github.com/sirupsen/logrus"

type logEntry struct {
	lvl     LogLevel
	msg     string
	args    []interface{}
	wrapper *logrusEntryWrapper
}

type bufStorage struct {
	logs     []logEntry
	wrappers map[*logrusEntryWrapper]struct{}
}

type bufLogger struct {
	wrapper    *logrusEntryWrapper
	coreLogger loggerImpl
	storage    *bufStorage
	flusher    flusher
}

var _ loggerImpl = new(bufLogger)

func newBufLogger(coreLogger *logrus.Logger) *bufLogger {
	return &bufLogger{
		storage: &bufStorage{
			logs:     make([]logEntry, 0),
			wrappers: make(map[*logrusEntryWrapper]struct{}),
		},
		coreLogger: coreLogger,
	}
}

// child creates and registers a wrapped logger implementation and returns a new buffered logger
// that will pass down the same storage and fallback fields for its children, if any.
func (buf *bufLogger) child(wrapper *logrusEntryWrapper) *bufLogger {
	buf.storage.wrappers[wrapper] = struct{}{}
	return &bufLogger{
		wrapper:    wrapper,
		coreLogger: buf.coreLogger,
		storage:    buf.storage,
		flusher:    buf.flusher,
	}
}

func (buf *bufLogger) Tracef(msg string, args ...interface{}) {
	buf.storage.logs = append(buf.storage.logs, logEntry{
		lvl:     TraceLevel,
		msg:     msg,
		args:    args,
		wrapper: buf.wrapper,
	})
}

func (buf *bufLogger) Debugf(msg string, args ...interface{}) {
	buf.storage.logs = append(buf.storage.logs, logEntry{
		lvl:     DebugLevel,
		msg:     msg,
		args:    args,
		wrapper: buf.wrapper,
	})
}

func (buf *bufLogger) Infof(msg string, args ...interface{}) {
	buf.storage.logs = append(buf.storage.logs, logEntry{
		lvl:     InfoLevel,
		msg:     msg,
		args:    args,
		wrapper: buf.wrapper,
	})
}

func (buf *bufLogger) Warnf(msg string, args ...interface{}) {
	buf.storage.logs = append(buf.storage.logs, logEntry{
		lvl:     WarningLevel,
		msg:     msg,
		args:    args,
		wrapper: buf.wrapper,
	})
}
func (buf *bufLogger) Errorf(msg string, args ...interface{}) {
	buf.storage.logs = append(buf.storage.logs, logEntry{
		lvl:     ErrorLevel,
		msg:     msg,
		args:    args,
		wrapper: buf.wrapper,
	})
}

func (buf *bufLogger) Fatal(args ...interface{}) {
	buf.flusher.Flush()
	buf.coreLogger.Fatal(args...)
}

func (buf *bufLogger) Fatalf(msg string, args ...interface{}) {
	buf.flusher.Flush()
	buf.coreLogger.Fatalf(msg, args...)
}

func (buf *bufLogger) Panicf(msg string, args ...interface{}) {
	buf.flusher.Flush()
	buf.coreLogger.Panicf(msg, args...)
}
