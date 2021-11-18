package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

func init() {
	defaultLogger = NewLegacyLogger()
}

// Naming as RSCLogger until we remove the old interface. When it happens, we can rename to Logger.
type RSCLogger interface {
	WithCorrelationID(correlationID string) Entry
	Copy() RSCLogger
	SetOutput(output io.Writer)
	OnConfigReload(level LogLevel)
	Entry
}

type Entry interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})
	Fatal(args ...interface{})

	WithField(key string, value interface{}) Entry
	WithFields(fields Fields) Entry
	SetLevel(level LogLevel)
	SetReportCaller(bool)
}
type logrusImpl struct {
	*logrus.Logger
}

type Fields map[string]interface{}

// Returns a new resulting instance of Fields
// with provided fields concatenated.
func (f Fields) Concat(fields Fields) Fields {
	result := Fields{}
	for key, value := range f {
		result[key] = value
	}
	for key, value := range fields {
		result[key] = value
	}
	return result
}

const correlationIDKey = "correlation_id"

type OutputFormat string

const (
	JSONFormat OutputFormat = "JSON"
	TextFormat OutputFormat = "TEXT"
)

func (o *OutputFormat) UnmarshalText(text []byte) (err error) {
	values := []OutputFormat{
		JSONFormat,
		TextFormat,
	}

	for _, currentValue := range values {
		if strings.EqualFold(string(text), string(currentValue)) {
			*o = currentValue
			return nil
		}
	}

	return fmt.Errorf("invalid Format value '%s'. Allowed values are %v", text, values)
}

type LogLevel string

const (
	TraceLevel   LogLevel = "TRACE"
	DebugLevel   LogLevel = "DEBUG"
	InfoLevel    LogLevel = "INFO"
	WarningLevel LogLevel = "WARN"
	ErrorLevel   LogLevel = "ERROR"
)

func (o *LogLevel) UnmarshalText(text []byte) (err error) {
	values := []LogLevel{
		TraceLevel,
		DebugLevel,
		InfoLevel,
		WarningLevel,
		ErrorLevel,
	}

	for _, currentValue := range values {
		if strings.EqualFold(string(text), string(currentValue)) {
			*o = currentValue
			return nil
		}
	}

	return fmt.Errorf("invalid Log Level value '%s'. Allowed values are %v", text, values)
}

type RSCLoggerOptions struct {
	Enabled  bool
	Output   LogOutputType
	Format   OutputFormat
	Level    LogLevel
	Filepath string
}

func NewRSCLogger(options RSCLoggerOptions,
	outputBuilder OutputBuilder,
) RSCLogger {

	if !options.Enabled {
		return NewLegacyLogger()
	}

	l := logrus.New()
	l.SetOutput(outputBuilder.Build(options.Output, options.Filepath))
	l.SetFormatter(getFormatter(options.Format))
	l.SetLevel(getLevel(options.Level))

	rscLogger := &logrusImpl{
		Logger: l,
	}

	return rscLogger
}

func DefaultLogger() RSCLogger {
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

func (l logrusImpl) WithField(key string, value interface{}) Entry {
	e := l.Logger.WithField(key, value)
	return logrusEntryWrapper{Entry: e}
}

func (l logrusImpl) WithFields(fields Fields) Entry {
	e := l.Logger.WithFields(logrus.Fields(fields))
	return logrusEntryWrapper{Entry: e}
}

func (l logrusImpl) WithCorrelationID(correlationID string) Entry {
	return logrusEntryWrapper{Entry: l.Logger.WithField(correlationIDKey, correlationID)}
}

func (l logrusImpl) Copy() RSCLogger {

	logrusCopy := logrus.New()
	logrusCopy.SetOutput(l.Out)

	logrusCopy.SetFormatter(l.Formatter)
	logrusCopy.SetLevel(l.GetLevel())

	copy := logrusImpl{
		Logger: logrusCopy,
	}

	return copy
}

func (l logrusImpl) SetLevel(level LogLevel) {
	logrusLevel := getLevel(level)
	l.Logger.SetLevel(logrusLevel)
}

func (l logrusImpl) SetOutput(output io.Writer) {
	l.Logger.SetOutput(output)
}

func (l logrusImpl) OnConfigReload(level LogLevel) {
	logrusLevel := getLevel(level)

	if logrusLevel != l.GetLevel() {
		l.SetLevel(level)
		l.Infof("Logging level changed to %s", level)
	}
}

func (l logrusImpl) SetReportCaller(flag bool) {
	l.Logger.SetReportCaller(flag)
}

type logrusEntryWrapper struct {
	*logrus.Entry
}

func (l logrusEntryWrapper) SetLevel(level LogLevel) {
	l.Entry.Logger.SetLevel(getLevel(level))
}

func (l logrusEntryWrapper) WithField(key string, value interface{}) Entry {
	e := l.Entry.WithField(key, value)
	return logrusEntryWrapper{Entry: e}
}

func (l logrusEntryWrapper) WithFields(fields Fields) Entry {
	e := l.Entry.WithFields(logrus.Fields(fields))
	return logrusEntryWrapper{Entry: e}
}

func (l logrusEntryWrapper) SetReportCaller(flag bool) {
	l.Logger.SetReportCaller(flag)
}

// TODO: Remove the legacyLogger struct and all its methods when we remove the logging enabled feature flag
type legacyLogger struct {
	preambleFields Fields
	reportCaller   bool
	debugLogger    *log.Logger
}

func NewLegacyLogger() RSCLogger {
	return &legacyLogger{
		preambleFields: Fields{},
		reportCaller:   false,
		debugLogger:    log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Llongfile),
	}
}

func (l *legacyLogger) handleOutput(msg string, args ...interface{}) {
	withPreamble := fieldsToPreamble(l.preambleFields) + msg
	if l.reportCaller {
		// Shared implementation for Logger so different entry points
		// have the same stack depth, allowing accurate file:lineno extraction.
		l.debugLogger.Output(3, fmt.Sprintf(withPreamble, args...))
	} else {
		log.Printf(withPreamble, args...)
	}
}

func (l *legacyLogger) Debugf(msg string, args ...interface{}) {
	if l.reportCaller {
		l.handleOutput(msg, args...)
	}
}

func (l *legacyLogger) Infof(msg string, args ...interface{}) {
	l.handleOutput(msg, args...)
}

func (l *legacyLogger) Warnf(msg string, args ...interface{}) {
	l.handleOutput(msg, args...)
}

func (l *legacyLogger) Errorf(msg string, args ...interface{}) {
	l.handleOutput(msg, args...)
}

func (l *legacyLogger) Fatalf(msg string, args ...interface{}) {
	log.Fatalf(msg, args...)
}

func (l *legacyLogger) Fatal(args ...interface{}) {
	log.Fatal(args...)
}

// This methods help replace PreambleLogger temporarily
func (l *legacyLogger) WithField(key string, value interface{}) Entry {
	return &legacyLogger{
		preambleFields: l.preambleFields.Concat(Fields{key: value}),
		reportCaller:   l.reportCaller,
		debugLogger:    l.debugLogger,
	}
}

// This methods help replace PreambleLogger temporarily
func (l *legacyLogger) WithFields(fields Fields) Entry {
	return &legacyLogger{
		preambleFields: l.preambleFields.Concat(fields),
		reportCaller:   l.reportCaller,
		debugLogger:    l.debugLogger,
	}
}

func (l *legacyLogger) SetReportCaller(flag bool) {
	l.reportCaller = flag
}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l *legacyLogger) WithCorrelationID(correlationID string) Entry {
	return l
}

func (l *legacyLogger) Copy() RSCLogger {
	return &legacyLogger{
		preambleFields: l.preambleFields,
		reportCaller:   l.reportCaller,
		debugLogger:    l.debugLogger,
	}
}

func (l *legacyLogger) SetLevel(level LogLevel) {
}

func (l legacyLogger) SetOutput(output io.Writer) {
}

func (l legacyLogger) OnConfigReload(level LogLevel) {
}

var defaultLogger RSCLogger

func SetDefaultLogger(logger RSCLogger) {
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

func WithField(key string, value interface{}) Entry {
	return defaultLogger.WithField(key, value)
}

func WithFields(fields Fields) Entry {
	return defaultLogger.WithFields(fields)
}

func WithCorrelationID(correlationID string) Entry {
	return defaultLogger.WithField(correlationIDKey, correlationID)
}
