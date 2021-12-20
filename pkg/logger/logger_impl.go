package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"io"

	"github.com/sirupsen/logrus"
)

const (
	ServerLog LogCategory = "SERVER"
)

func init() {
	// Set a default logger on init. This is mainly to prevent test failures since
	// the default logger would otherwise be unset.
	defaultLogger, _ = NewLoggerImpl(LoggerOptionsImpl{
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
}

type LoggerImpl struct {
	*logrus.Logger
}

type OutputDest struct {
	Output      LogOutputType
	Filepath    string
	DefaultFile string
}

type LoggerOptionsImpl struct {
	Output []OutputDest
	Level  LogLevel
	Format OutputFormat
}

func NewLoggerImpl(options LoggerOptionsImpl,
	outputBuilder OutputBuilder,
) (*LoggerImpl, error) {
	var output []io.Writer

	l := logrus.New()

	for _, out := range options.Output {
		wrtr, err := outputBuilder.Build(out.Output, out.Filepath)
		if err != nil {
			return nil, err
		}
		output = append(output, wrtr)
	}

	l.SetOutput(io.MultiWriter(output...))
	l.SetFormatter(getFormatter(options.Format))
	l.SetLevel(getLevel(options.Level))

	return &LoggerImpl{
		Logger: l,
	}, nil
}

func DefaultLogger() Logger {
	return defaultLogger
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

func (l LoggerImpl) Copy() Logger {

	logrusCopy := logrus.New()
	logrusCopy.SetOutput(l.Out)

	logrusCopy.SetFormatter(l.Formatter)
	logrusCopy.SetLevel(l.GetLevel())

	return LoggerImpl{
		Logger: logrusCopy,
	}
}

func (l LoggerImpl) SetLevel(level LogLevel) {
	logrusLevel := getLevel(level)
	l.Logger.SetLevel(logrusLevel)
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

func (l LoggerImpl) SetReportCaller(flag bool) {
	l.Logger.SetReportCaller(flag)
}

type logrusEntryWrapper struct {
	*logrus.Entry
}

func (l logrusEntryWrapper) Copy() Logger {

	entryCopy := logrus.NewEntry(l.Logger)
	entryCopy.Data = l.Data
	entryCopy.Context = l.Context

	return logrusEntryWrapper{
		Entry: entryCopy,
	}
}

func (l logrusEntryWrapper) SetLevel(level LogLevel) {
	l.Entry.Logger.SetLevel(getLevel(level))
}

func (l logrusEntryWrapper) SetOutput(writers ...io.Writer) {
	output := io.MultiWriter(writers...)
	l.Entry.Logger.SetOutput(output)
}

func (l logrusEntryWrapper) WithField(key string, value interface{}) Logger {
	e := l.Entry.WithField(key, value)
	return logrusEntryWrapper{Entry: e}
}

func (l logrusEntryWrapper) WithFields(fields Fields) Logger {
	e := l.Entry.WithFields(logrus.Fields(fields))
	return logrusEntryWrapper{Entry: e}
}

func (l logrusEntryWrapper) SetReportCaller(flag bool) {
	l.Logger.SetReportCaller(flag)
}

// TODO: remove this function when the migration process to the new logging standard is complete.
func (l logrusEntryWrapper) Logf(msg string, args ...interface{}) {
	l.Infof(msg, args...)
}

var defaultLogger Logger

func SetDefaultLogger(logger Logger) {
	defaultLogger = logger
}

func Debugf(msg string, args ...interface{}) {
	defaultLogger.Debugf(msg, args...)
}

func Infof(msg string, args ...interface{}) {
	defaultLogger.Infof(msg, args...)
}

func Warnf(msg string, args ...interface{}) {
	defaultLogger.Warnf(msg, args...)
}

func Errorf(msg string, args ...interface{}) {
	defaultLogger.Errorf(msg, args...)
}

func Fatal(msg string) {
	defaultLogger.Fatal(msg)
}

func Fatalf(msg string, args ...interface{}) {
	defaultLogger.Fatalf(msg, args...)
}

func WithField(key string, value interface{}) Logger {
	return defaultLogger.WithField(key, value)
}

func WithFields(fields Fields) Logger {
	return defaultLogger.WithFields(fields)
}
