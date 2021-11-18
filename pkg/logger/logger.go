package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
)

// for testing.
var _log_printf = log.Printf

// Logger logs things, one line at a time. Most implementations send log lines
// to the console.
type Logger interface {
	Logf(msg string, args ...interface{})
}

// Outputter records output that is not sent to the console (e.g. process
// output from deploy tasks).
type Outputter interface {
	Output(msg string)
}

// OutputLogger is a logger with an extra interface that can be used for
// recording output that is not sent to console.
type OutputLogger interface {
	Logger
	Outputter
}

// A logger that drops all messages. Useful for tests.
type discardLogger struct{}

func (discardLogger) Logf(msg string, args ...interface{})   {}
func (discardLogger) Debugf(msg string, args ...interface{}) {}
func (discardLogger) Infof(msg string, args ...interface{})  {}
func (discardLogger) Warnf(msg string, args ...interface{})  {}
func (discardLogger) Errorf(msg string, args ...interface{}) {}
func (discardLogger) Fatalf(msg string, args ...interface{}) {}
func (discardLogger) Fatal(args ...interface{})              {}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithField(key string, value interface{}) Entry {
	return l
}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithFields(fields Fields) Entry {
	return l
}

// Doing nothing with the provided values. Just to implement the interface and be able to switch to old logging implementation
func (l discardLogger) WithCorrelationID(correlationID string) Entry {
	return l
}

func (l discardLogger) Copy() RSCLogger {
	return l
}

func (discardLogger) SetLevel(level LogLevel)       {}
func (discardLogger) SetOutput(output io.Writer)    {}
func (discardLogger) OnConfigReload(level LogLevel) {}
func (discardLogger) SetReportCaller(flag bool)     {}

var DiscardLogger discardLogger = discardLogger{}

type discardOutputter struct{}

func (discardOutputter) Output(msg string) {}

// An output logger that drops all messages. Useful for tests.
type discardOutputLogger struct {
	discardLogger
	discardOutputter
}

var DiscardOutputLogger = discardOutputLogger{}

// A logger that directly sends to `log`
type directLogger struct{}

func (logger directLogger) Logf(msg string, args ...interface{}) {
	_log_printf(msg, args...)
}

var DirectLogger directLogger = directLogger{}

// A logger that directly sends log statements and output to an io.Writer
type WriterLogger struct {
	Writer io.Writer
}

func NewWriterLogger(writer io.Writer) WriterLogger {
	return WriterLogger{
		Writer: writer,
	}
}

func (logger WriterLogger) Log(msg string) {
	logger.Output(msg)
}
func (logger WriterLogger) Logf(msg string, args ...interface{}) {
	logger.Output(fmt.Sprintf(msg, args...))
}
func (logger WriterLogger) Output(msg string) {
	fmt.Fprint(logger.Writer, msg+"\n")
}

// BuildPreamble constructs a logging prefix string from a set of input values.
// Most times, you will give an even number of values, which generate a prefix like:
//     [v1: v2; v3: v4]
//
// If an odd number of fields is given, the last field is presented by itself:
//     [v1: v2; v3: v4; v5]
func BuildPreamble(fields ...interface{}) string {
	n := len(fields)
	if n == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString("[")
	for i := 0; i < n; i++ {
		fmt.Fprint(&b, fields[i])
		// odd-indexed fields are followed by a semi-colon,
		// terminating the pair, unless this is the final field.
		//
		// even-indexed fields are followed by a colon, unless this is
		// the final field (this is an unbalanced pair).
		if i != n-1 {
			if i%2 == 0 {
				b.WriteString(": ")
			} else {
				b.WriteString("; ")
			}
		}
	}
	b.WriteString("] ")

	return b.String()
}

func getOrderedKey(fields Fields) []string {
	keys := make([]string, len(fields))

	i := 0
	for key := range fields {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	return keys
}

func fieldsToPreamble(fields Fields) string {
	var preambleArgs []interface{}
	keys := getOrderedKey(fields)
	for _, key := range keys {
		preambleArgs = append(preambleArgs, key, fields[key])
	}
	return BuildPreamble(preambleArgs...)
}

// A logger with a "[app: XXX]" preamble.
// Satisfies the ReportLogger interface.
type PreambleLogger struct {
	Preamble string
	Output   Logger
}

func (logger PreambleLogger) Logf(msg string, args ...interface{}) {
	if logger.Output != nil {
		logger.Output.Logf(logger.Preamble+msg, args...)
	} else {
		_log_printf(logger.Preamble+msg, args...)
	}
}

// RecordingLogger is a direct logger that remembers its last Logf call and
// its rendered result. Useful for tests.
type RecordingLogger struct {
	directLogger
	Called  bool
	Format  string
	Args    []interface{}
	Message string
}

// Reset clears any previously recorded state.
func (logger *RecordingLogger) Reset() {
	logger.Called = false
	logger.Format = ""
	logger.Args = nil
	logger.Message = ""
}

func (logger *RecordingLogger) Logf(msg string, args ...interface{}) {
	logger.Called = true
	logger.Format = msg
	logger.Args = args
	logger.Message = fmt.Sprintf(msg, args...)
	logger.directLogger.Logf(msg, args...)
}

// CapturingLogger is a direct logger that remembers _all_ its logged
// messages. Useful for tests.
type CapturingLogger struct {
	directLogger
	Messages []string
}

func (logger *CapturingLogger) Logf(msg string, args ...interface{}) {
	logger.Messages = append(logger.Messages,
		fmt.Sprintf(msg, args...),
	)
	logger.directLogger.Logf(msg, args...)
}

func (logger *CapturingLogger) Output(msg string) {
	logger.Messages = append(logger.Messages, msg)
}

func (logger *CapturingLogger) Reset() {
	logger.Messages = nil
}

// CapturingPassthroughLogger is a logger that remembers _all_ its logged
// messages and passes them through to a parent logger.
type CapturingPassthroughLogger struct {
	parent   Logger
	Messages []string
}

func NewCapturingPassthroughLogger(parent Logger) *CapturingPassthroughLogger {
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

// CaptureOnlyLogger is a logger that remembers its logged
// messages, but doesn't log them anywhere else.
type CaptureOnlyLogger struct {
	Messages []string
}

func (logger *CaptureOnlyLogger) Logf(msg string, args ...interface{}) {
	logger.Output(fmt.Sprintf(msg, args...))
}

func (logger *CaptureOnlyLogger) Output(msg string) {
	logger.Messages = append(logger.Messages, msg)
}

func (logger *CaptureOnlyLogger) Reset() {
	logger.Messages = nil
}

// MultiLogf performs a Logf for each non-nil logger with the same format string
// and variadic args
func MultiLogf(loggers []Logger, format string, v ...interface{}) {
	for _, l := range loggers {
		if l != nil {
			l.Logf(format, v...)
		}
	}
}
