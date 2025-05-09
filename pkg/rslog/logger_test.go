package rslog

// Copyright (C) 2022 by RStudio, PBC.

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestLoggerSuite(t *testing.T) {
	suite.Run(t, &LoggerSuite{})
}

type LoggerSuite struct {
	suite.Suite
}

func (s *LoggerSuite) TestLoggers() {
	original_printf := _log_printf
	defer func() { _log_printf = original_printf }()

	fin := "here we %d are now; %s"
	vin := []interface{}{
		42,
		"entertain us",
	}

	_log_printf = func(f string, v ...interface{}) {
		s.Fail("discard should not log")
	}
	DiscardLogger.Logf(fin, vin...)

	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, fin)
		s.ElementsMatch(v, vin)
	}
	DirectLogger.Logf(fin, vin...)

	logger := PreambleLogger{
		Preamble: "[grumpy] ",
	}

	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, "[grumpy] "+fin)
		s.ElementsMatch(v, vin)
	}
	logger.Logf(fin, vin...)

	// Alternative output
	logger.Output = DirectLogger
	logger.Logf(fin, vin...)

	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, fin)
		s.ElementsMatch(v, vin)
	}

	var rec DeprecatedRecordingLogger
	rec.Logf(fin, vin...)
	s.Equal(rec.Called, true)
	s.Equal(rec.Format, fin)
	s.ElementsMatch(rec.Args, vin)
	s.Equal(rec.Message, "here we 42 are now; entertain us")

	rec.Reset()
	s.True(reflect.DeepEqual(rec, DeprecatedRecordingLogger{}))

	var cap DeprecatedCapturingLogger
	var capm string
	var capa []interface{}
	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, capm)
		s.ElementsMatch(v, capa)
	}

	capm = "this is the %s that never %s"
	capa = []interface{}{"song", "ends"}
	cap.Logf(capm, capa...)
	capm = "it just goes %s and %s my %s"
	capa = []interface{}{"on", "on", "friends"}
	cap.Logf(capm, capa...)
	s.ElementsMatch(cap.Messages, []string{
		"this is the song that never ends",
		"it just goes on and on my friends",
	})

	cap.Reset()
	s.True(reflect.DeepEqual(cap, DeprecatedCapturingLogger{}))

	var capOnly DeprecatedCaptureOnlyLogger
	var capMsg string
	var capArg []interface{}
	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, capMsg)
		s.ElementsMatch(v, capArg)
	}

	capMsg = "this is the %s that never %s"
	capArg = []interface{}{"song", "ends"}
	capOnly.Logf(capMsg, capArg...)
	capMsg = "it just goes %s and %s my %s"
	capArg = []interface{}{"on", "on", "friends"}
	capOnly.Logf(capMsg, capArg...)
	s.ElementsMatch(capOnly.Messages, []string{
		"this is the song that never ends",
		"it just goes on and on my friends",
	})

	capOnly.Reset()
	s.True(reflect.DeepEqual(capOnly, DeprecatedCaptureOnlyLogger{}))

	_log_printf = func(f string, v ...interface{}) {
		s.Equal(f, capMsg)
		s.ElementsMatch(v, capArg)
	}
}

func (s *LoggerSuite) TestBuildPreamble() {
	for _, each := range []struct {
		description string
		args        []interface{}
		expected    string
	}{
		{
			description: "empty arguments",
			args:        nil,
			expected:    "",
		}, {
			description: "one string argument",
			args: []interface{}{
				"first",
			},
			expected: "[first] ",
		}, {
			description: "two string arguments",
			args: []interface{}{
				"first", "first-value",
			},
			expected: "[first: first-value] ",
		}, {
			description: "three string arguments",
			args: []interface{}{
				"first", "first-value",
				"second",
			},
			expected: "[first: first-value; second] ",
		}, {
			description: "four string arguments",
			args: []interface{}{
				"first", "first-value",
				"second", "second-value",
			},
			expected: "[first: first-value; second: second-value] ",
		}, {
			description: "one non-string argument",
			args: []interface{}{
				42,
			},
			expected: "[42] ",
		}, {
			description: "two non-string arguments",
			args: []interface{}{
				42, 13,
			},
			expected: "[42: 13] ",
		}, {
			description: "three non-string arguments",
			args: []interface{}{
				42, 13, 101,
			},
			expected: "[42: 13; 101] ",
		}, {
			description: "four non-string arguments",
			args: []interface{}{
				42, 13, 101, 17,
			},
			expected: "[42: 13; 101: 17] ",
		}, {
			description: "two mixed arguments",
			args: []interface{}{
				"first", 42,
			},
			expected: "[first: 42] ",
		}, {
			description: "three mixed arguments",
			args: []interface{}{
				"first", 42,
				"second",
			},
			expected: "[first: 42; second] ",
		}, {
			description: "four mixed arguments",
			args: []interface{}{
				"first", 42,
				"second", 13,
			},
			expected: "[first: 42; second: 13] ",
		},
	} {
		s.Equal(BuildPreamble(each.args...), each.expected, each.description)
	}
}

func toPtr(level LogLevel) *LogLevel {
	return &level
}

func (s *LoggerSuite) TesteCompareLevels() {
	s.Equal(toPtr(TraceLevel).Compare(TraceLevel), 0)
	s.Equal(toPtr(ErrorLevel).Compare(ErrorLevel), 0)
	s.Equal(toPtr(TraceLevel).Compare(DebugLevel), 1)
	s.Equal(toPtr(TraceLevel).Compare(ErrorLevel), 1)
	s.Equal(toPtr(ErrorLevel).Compare(TraceLevel), -1)
	s.Equal(toPtr(ErrorLevel).Compare(WarningLevel), -1)
}
