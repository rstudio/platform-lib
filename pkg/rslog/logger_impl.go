package rslog

// Copyright (C) 2021 by RStudio, PBC.

import (
	"io"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	ServerLog LogCategory = "SERVER"
)

var DefaultLoggerFactory LoggerFactory

type LoggerFactoryImpl struct{}

func (f *LoggerFactoryImpl) DefaultLogger() Logger {
	lgr, _ := NewLoggerImpl(LoggerOptionsImpl{
		Output: []OutputDest{
			{
				LogOutputStdout,
				"",
				"",
			},
		},
		Format: TextFormat,
		Level:  InfoLevel,
	}, NewOutputLogBuilder(ServerLog, ""))
	return lgr
}

func (f *LoggerFactoryImpl) TerminalLogger(l LogLevel) Logger {
	lgr, _ := NewLoggerImpl(LoggerOptionsImpl{
		Format: TextFormat,
		Level:  l,
	}, StderrOutputBuilder{})
	return lgr
}

type LoggerImpl struct {
	*logrus.Logger
}

type LoggerOptionsImpl struct {
	Output []OutputDest
	Level  LogLevel
	Format OutputFormat
}

func NewLoggerImpl(options LoggerOptionsImpl,
	outputBuilder OutputBuilder,
) (*LoggerImpl, error) {

	l := logrus.New()

	writer, err := outputBuilder.Build(options.Output...)
	if err != nil {
		return nil, err
	}

	l.SetOutput(writer)
	l.SetFormatter(getFormatter(options.Format))
	l.SetLevel(getLevel(options.Level))

	return &LoggerImpl{
		Logger: l,
	}, nil
}

func getFormatter(outputFormat OutputFormat) logrus.Formatter {
	switch outputFormat {
	case JSONFormat:
		return &logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z"}
	default:
		return &logrus.TextFormatter{
			FullTimestamp:          true,
			TimestampFormat:        "2006-01-02T15:04:05.000Z",
			DisableLevelTruncation: true,
		}
	}
}

func getLevel(level LogLevel) logrus.Level {
	switch level {
	case TraceLevel:
		return logrus.TraceLevel
	case DebugLevel:
		return logrus.DebugLevel
	case InfoLevel:
		return logrus.InfoLevel
	case WarningLevel:
		return logrus.WarnLevel
	case ErrorLevel:
		return logrus.ErrorLevel
	default:
		return logrus.InfoLevel
	}
}

func (l LoggerImpl) WithField(key string, value interface{}) Logger {
	e := l.Logger.WithField(key, value)
	return logrusEntryWrapper{Entry: e}
}

func (l LoggerImpl) WithFields(fields Fields) Logger {
	e := l.Logger.WithFields(logrus.Fields(fields))
	return logrusEntryWrapper{Entry: e}
}

func (l LoggerImpl) SetLevel(level LogLevel) {
	logrusLevel := getLevel(level)
	l.Logger.SetLevel(logrusLevel)
}

func (l LoggerImpl) SetFormatter(format OutputFormat) {
	l.Logger.SetFormatter(getFormatter(format))
}

func (l LoggerImpl) SetOutput(writers ...io.Writer) {
	output := io.MultiWriter(writers...)
	l.Logger.SetOutput(output)
}

func (l LoggerImpl) OnConfigReload(level LogLevel) {
	logrusLevel := getLevel(level)

	if logrusLevel != l.GetLevel() {
		l.SetLevel(level)
		l.Infof("Logging level changed to %s", level)
	}
}

// TODO: remove this function when the migration process to the new logging standard is complete.
func (l LoggerImpl) Logf(msg string, args ...interface{}) {
	l.Infof(msg, args...)
}

type logrusEntryWrapper struct {
	*logrus.Entry
}

func (l logrusEntryWrapper) SetLevel(level LogLevel) {
	l.Entry.Logger.SetLevel(getLevel(level))
}

func (l logrusEntryWrapper) SetOutput(writers ...io.Writer) {
	output := io.MultiWriter(writers...)
	l.Entry.Logger.SetOutput(output)
}

func (l logrusEntryWrapper) SetFormatter(format OutputFormat) {
	l.Entry.Logger.SetFormatter(getFormatter(format))
}

func (l logrusEntryWrapper) WithField(key string, value interface{}) Logger {
	e := l.Entry.WithField(key, value)
	return logrusEntryWrapper{Entry: e}
}

func (l logrusEntryWrapper) WithFields(fields Fields) Logger {
	e := l.Entry.WithFields(logrus.Fields(fields))
	return logrusEntryWrapper{Entry: e}
}

// TODO: remove this function when the migration process to the new logging standard is complete.
func (l logrusEntryWrapper) Logf(msg string, args ...interface{}) {
	l.Infof(msg, args...)
}

var defaultLogger Logger
var once = &sync.Once{}
var mutex sync.RWMutex

func ensureDefaultLoggerReadLock() *sync.RWMutex {
	// Set the default logger only once.
	once.Do(func() {
		mutex.Lock()
		defer mutex.Unlock()

		// Create default factory if not already set
		if DefaultLoggerFactory == nil {
			DefaultLoggerFactory = &LoggerFactoryImpl{}
		}

		// Set default logger
		defaultLogger = DefaultLoggerFactory.DefaultLogger()
	})

	mutex.RLock()
	return &mutex
}

// UpdateDefaultLogger should be the only way to update the default logger.
func UpdateDefaultLogger(options LoggerOptionsImpl, outputBuilder OutputBuilder) error {
	w, err := outputBuilder.Build(options.Output...)
	if err != nil {
		return err
	}

	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.SetOutput(w)
	defaultLogger.SetFormatter(options.Format)
	defaultLogger.SetLevel(options.Level)
	return nil
}

// ReplaceDefaultLogger replaces the default logger. This is not ideal, but was included
// to support switching to a legacy logger.
// TODO: Remove this when all applications have fully moved to the new logging standard.
func ReplaceDefaultLogger(logger Logger) {
	mutex.Lock()
	defer mutex.Unlock()
	defaultLogger = logger
}

// UseTerminalLogger makes the default logger have its output to STDERR
// with enhanced text formatting
func UseTerminalLogger(l LogLevel) {
	once.Do(func() {
		mutex.Lock()
		defer mutex.Unlock()

		// Create default factory if not already set
		if DefaultLoggerFactory == nil {
			DefaultLoggerFactory = &LoggerFactoryImpl{}
		}

		// Set default logger
		defaultLogger = DefaultLoggerFactory.TerminalLogger(l)
	})

}

func DefaultLogger() Logger {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	return defaultLogger
}

func Debugf(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Debugf(msg, args...)
}

func Tracef(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Tracef(msg, args...)
}

func Infof(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Infof(msg, args...)
}

func Warnf(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Warnf(msg, args...)
}

func Errorf(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Errorf(msg, args...)
}

func Fatal(msg string) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Fatal(msg)
}

func Fatalf(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Fatalf(msg, args...)
}

func Panicf(msg string, args ...interface{}) {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	defaultLogger.Panicf(msg, args...)
}

func WithField(key string, value interface{}) Logger {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	return defaultLogger.WithField(key, value)
}

func WithFields(fields Fields) Logger {
	lock := ensureDefaultLoggerReadLock()
	defer lock.RUnlock()
	return defaultLogger.WithFields(fields)
}
