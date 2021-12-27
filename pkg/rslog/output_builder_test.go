package rslog

import (
	"errors"
	"os"

	"github.com/stretchr/testify/mock"
)

// Copyright (C) 2021 by RStudio, PBC.

func (s *LoggerSuite) TestBuildStdout() {
	builder := outputBuilder{
		fileSystem: &MockFileSystem{},
	}

	result, err := builder.Build(LogOutputStdout, "")
	s.Nil(err)
	s.Equal(result, os.Stdout)
}

func (s *LoggerSuite) TestBuildEmptyFile() {

	mockFileSystem := &MockFileSystem{}
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", "/default/log/logfile.log").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/default/log/logfile.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, nil)

	result, err := builder.Build(LogOutputFile, "")
	s.Nil(err)
	s.Equal(result, defaultLogPath)
}

func (s *LoggerSuite) TestBuildEmptyFileFallbackErr() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", "/default/log/logfile.log").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/default/log/logfile.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, errors.New("Error"))

	_, err := builder.Build(LogOutputFile, "")
	s.NotNil(err)
}

func (s *LoggerSuite) TestBuildOpenFileError() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, nil)
	mockFileSystem.On("Stat", "/default/log").Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(new(os.File), errors.New("Error"))

	mockFileSystem.On(
		"OpenFile",
		"/default/log/logfile.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, nil)

	result, err := builder.Build(LogOutputFile, "/custom/test.log")
	s.Nil(err)
	s.Equal(result, defaultLogPath)
}

func (s *LoggerSuite) TestConfigureLogFileOutput() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	customLogPath := new(os.File)

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(customLogPath, nil)

	result, err := builder.Build(LogOutputFile, "/custom/test.log")
	s.Nil(err)
	s.Equal(result, customLogPath)
}

func (s *LoggerSuite) TestConfigureLogDefaultLoggingNonExistingDirectory() {
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, os.ErrNotExist)
	mockFileSystem.On("MkdirAll", "/custom", os.FileMode(0777)).Return(nil)
	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, nil)

	result, err := builder.Build(LogOutputFile, "/custom/test.log")
	s.Nil(err)
	s.Equal(result, defaultLogPath)
}

func (s *LoggerSuite) TestConfigureLogStderr() {
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	result, err := builder.Build(LogOutputStderr, "")
	s.Nil(err)
	s.Equal(result, os.Stderr)
}

func (s *LoggerSuite) TestConfigureLogDefault() {
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	result, err := builder.Build(LogOutputDefault, "")
	s.Nil(err)
	s.Equal(result, os.Stderr)
}

func (s *LoggerSuite) TestLogOutputTypeUnmarshalText() {
	testCases := []struct {
		TestName        string
		ValueToConvert  string
		ExpectedValue   LogOutputType
		ShouldHaveError bool
	}{
		{
			TestName:        "Unmarshal STDOUT LogOutputType value",
			ValueToConvert:  string(LogOutputStdout),
			ExpectedValue:   LogOutputStdout,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal STDERR LogOutputType value",
			ValueToConvert:  string(LogOutputStderr),
			ExpectedValue:   LogOutputStderr,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal FILE LogOutputType value",
			ValueToConvert:  string(LogOutputFile),
			ExpectedValue:   LogOutputFile,
			ShouldHaveError: false,
		},
		{
			TestName:        "Unmarshal empty LogOutputType value",
			ValueToConvert:  "",
			ShouldHaveError: true,
		},
		{
			TestName:        "Unmarshal invalid LogOutputType value",
			ValueToConvert:  "anything",
			ShouldHaveError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.TestName, func() {
			var result LogOutputType
			err := result.UnmarshalText([]byte(tc.ValueToConvert))

			if tc.ShouldHaveError {
				s.NotNil(err)
			} else {
				s.Nil(err)
				s.Equal(tc.ExpectedValue, result)
			}
		})
	}
}

type MockFileSystem struct {
	mock.Mock
}

func (o *MockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	args := o.Called(path, perm)
	return args.Error(0)
}

func (o *MockFileSystem) Stat(path string) (os.FileInfo, error) {
	args := o.Called(path)
	return args.Get(0).(os.FileInfo), args.Error(1)
}
func (o *MockFileSystem) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	args := o.Called(name, flag, perm)
	return args.Get(0).(*os.File), args.Error(1)
}

type mockFileInfo struct {
	// Embedding the interface like that allows to implement only the needed methods in the test functions
	os.FileInfo
}
