package rslog

import "io"

// Copyright (C) 2022 by RStudio, PBC.

type CompositeLogger struct {
	loggers []Logger
}

var _ Logger = &CompositeLogger{}

// ComposeLoggers composes multiples loggers in just one.
func ComposeLoggers(loggers ...Logger) *CompositeLogger {
	return &CompositeLogger{
		loggers: loggers,
	}
}

func (l *CompositeLogger) Logf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Logf(msg, args...)
	}
}

func (l *CompositeLogger) Tracef(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Tracef(msg, args...)
	}
}

func (l *CompositeLogger) Debugf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Debugf(msg, args...)
	}
}

func (l *CompositeLogger) Infof(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Infof(msg, args...)
	}
}

func (l *CompositeLogger) Warnf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Warnf(msg, args...)
	}
}

func (l *CompositeLogger) Errorf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Errorf(msg, args...)
	}
}

func (l *CompositeLogger) Fatalf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Fatalf(msg, args...)
	}
}

func (l *CompositeLogger) Fatal(args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Fatal(args...)
	}
}

func (l *CompositeLogger) Panicf(msg string, args ...interface{}) {
	for _, logger := range l.loggers {
		logger.Panicf(msg, args...)
	}
}

// Writer returns an io.WriteCloser, that contains multiple writers inside it.
// It makes possible to see the multiple writers being returned as just one.
func (l *CompositeLogger) Writer() io.WriteCloser {

	writers := make([]io.WriteCloser, 0, len(l.loggers))

	for _, logger := range l.loggers {
		writers = append(writers, logger.Writer())
	}

	return NewMultiWriteCloser(writers...)
}

func (l *CompositeLogger) WithField(key string, value interface{}) Logger {
	newComposite := new(CompositeLogger)

	for _, logger := range l.loggers {
		newLogger := logger.WithField(key, value)
		newComposite.loggers = append(newComposite.loggers, newLogger)
	}
	return newComposite
}

func (l *CompositeLogger) WithFields(fields Fields) Logger {
	newComposite := new(CompositeLogger)

	for _, logger := range l.loggers {
		newLogger := logger.WithFields(fields)
		newComposite.loggers = append(newComposite.loggers, newLogger)
	}
	return newComposite
}

func (l *CompositeLogger) SetLevel(level LogLevel) {
	for _, logger := range l.loggers {
		logger.SetLevel(level)
	}
}

func (l *CompositeLogger) SetOutput(writers ...io.Writer) {
	for _, logger := range l.loggers {
		logger.SetOutput(writers...)
	}
}

func (l *CompositeLogger) SetFormatter(formatter OutputFormat) {
	for _, logger := range l.loggers {
		logger.SetFormatter(formatter)
	}
}

func NewMultiWriteCloser(writers ...io.WriteCloser) io.WriteCloser {
	allWriters := make([]io.WriteCloser, 0, len(writers))
	for _, w := range writers {
		if mw, ok := w.(*multiWriteCloser); ok {
			allWriters = append(allWriters, mw.writeClosers...)
		} else {
			allWriters = append(allWriters, w)
		}
	}
	return &multiWriteCloser{allWriters}
}

type multiWriteCloser struct {
	writeClosers []io.WriteCloser
}

func (mw *multiWriteCloser) Close() error {
	var err error = nil
	for _, c := range mw.writeClosers {
		err = c.Close()
	}
	return err
}

func (mw *multiWriteCloser) Write(p []byte) (n int, err error) {
	for _, w := range mw.writeClosers {
		n, err = w.Write(p)
		if err != nil {
			return
		}
		if n != len(p) {
			err = io.ErrShortWrite
			return
		}
	}
	return len(p), nil
}
