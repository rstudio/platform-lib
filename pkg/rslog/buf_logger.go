package rslog

// Copyright (C) 2022 by RStudio, PBC.

type BufLogEntry struct {
	Level   LogLevel
	Message string
	Args    []interface{}
	Logger  CoreLoggerImpl
}

type BufStorage struct {
	Logs []BufLogEntry
}

type BufLogger struct {
	CoreLogger CoreLoggerImpl
	Storage    *BufStorage
}

var _ CoreLoggerImpl = new(BufLogger)

func NewBufLogger(coreLogger CoreLoggerImpl) *BufLogger {
	return &BufLogger{
		Storage: &BufStorage{
			Logs: make([]BufLogEntry, 0),
		},
		CoreLogger: coreLogger,
	}
}

// child creates and registers a wrapped logger implementation and returns a new buffered logger
// that will pass down the same storage and fallback fields for its children, if any.
func (buf *BufLogger) child(coreLogger CoreLoggerImpl) *BufLogger {
	return &BufLogger{
		CoreLogger: coreLogger,
		Storage:    buf.Storage,
	}
}

func (buf *BufLogger) Tracef(msg string, args ...interface{}) {
	buf.Storage.Logs = append(buf.Storage.Logs, BufLogEntry{
		Level:   TraceLevel,
		Message: msg,
		Args:    args,
		Logger:  buf.CoreLogger,
	})
}

func (buf *BufLogger) Debugf(msg string, args ...interface{}) {
	buf.Storage.Logs = append(buf.Storage.Logs, BufLogEntry{
		Level:   DebugLevel,
		Message: msg,
		Args:    args,
		Logger:  buf.CoreLogger,
	})
}

func (buf *BufLogger) Infof(msg string, args ...interface{}) {
	buf.Storage.Logs = append(buf.Storage.Logs, BufLogEntry{
		Level:   InfoLevel,
		Message: msg,
		Args:    args,
		Logger:  buf.CoreLogger,
	})
}

func (buf *BufLogger) Warnf(msg string, args ...interface{}) {
	buf.Storage.Logs = append(buf.Storage.Logs, BufLogEntry{
		Level:   WarningLevel,
		Message: msg,
		Args:    args,
		Logger:  buf.CoreLogger,
	})
}
func (buf *BufLogger) Errorf(msg string, args ...interface{}) {
	buf.Storage.Logs = append(buf.Storage.Logs, BufLogEntry{
		Level:   ErrorLevel,
		Message: msg,
		Args:    args,
		Logger:  buf.CoreLogger,
	})
}

func (buf *BufLogger) Fatal(args ...interface{}) {
	if f, ok := buf.CoreLogger.(Flusher); ok {
		f.Flush()
	}
	buf.CoreLogger.Fatal(args...)
}

func (buf *BufLogger) Fatalf(msg string, args ...interface{}) {
	if f, ok := buf.CoreLogger.(Flusher); ok {
		f.Flush()
	}
	buf.CoreLogger.Fatalf(msg, args...)
}

func (buf *BufLogger) Panicf(msg string, args ...interface{}) {
	if f, ok := buf.CoreLogger.(Flusher); ok {
		f.Flush()
	}
	buf.CoreLogger.Panicf(msg, args...)
}
