package logger

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

	result := builder.Build(LogOutputStdout, "")

	s.Equal(result, os.Stdout)
}

func (s *LoggerSuite) TestBuildEmptyFile() {

	mockFileSystem := &MockFileSystem{}
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		DefaultFileLoggingPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, nil)

	result := builder.Build(LogOutputFile, "")

	s.Equal(result, defaultLogPath)
}

func (s *LoggerSuite) TestBuildEmptyFileFallback() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	file := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		DefaultFileLoggingPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(file, errors.New("Error"))

	result := builder.Build(LogOutputFile, "")

	s.Equal(result, os.Stdout)
}

func (s *LoggerSuite) TestBuildAccessFileFallback() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		logCategory: AccessLog,
		fileSystem:  mockFileSystem,
	}

	file := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		DefaultFileAccessLogPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(file, nil)

	result := builder.Build(LogOutputFile, "")

	s.Equal(result, file)
}

func (s *LoggerSuite) TestBuildAuditFileFallback() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		logCategory: AuditLog,
		fileSystem:  mockFileSystem,
	}

	file := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		DefaultFileAuditLogPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(file, nil)

	result := builder.Build(LogOutputFile, "")

	s.Equal(result, file)
}

func (s *LoggerSuite) TestBuildOpenFileError() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	defaultLogPath := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		"test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(new(os.File), errors.New("Error"))

	mockFileSystem.On(
		"OpenFile",
		DefaultFileLoggingPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(defaultLogPath, nil)

	result := builder.Build(LogOutputFile, "test.log")

	s.Equal(result, defaultLogPath)
}

func (s *LoggerSuite) TestConfigureLogFileOutput() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	file := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	mockFileSystem.On(
		"OpenFile",
		"test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(file, nil)

	result := builder.Build(LogOutputFile, "test.log")

	s.Equal(result, file)
}

func (s *LoggerSuite) TestConfigureLogDefaultLoggingExistingDirectory() {
	filepath := DefaultFileLoggingPath
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	expectedFile := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		filepath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(expectedFile, nil)

	result := builder.Build(LogOutputFile, filepath)

	s.Equal(result, expectedFile)
}

func (s *LoggerSuite) TestConfigureLogDefaultLoggingNonExistingDirectory() {
	filepath := DefaultFileLoggingPath
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	expectedFile := new(os.File)

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, os.ErrNotExist)
	mockFileSystem.On("MkdirAll", DefaultLoggingDir, os.FileMode(0777)).Return(nil)
	mockFileSystem.On(
		"OpenFile",
		filepath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(expectedFile, nil)

	result := builder.Build(LogOutputFile, filepath)

	s.Equal(result, expectedFile)
}

func (s *LoggerSuite) TestConfigureLogStderr() {
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	result := builder.Build(LogOutputStderr, "")

	s.Equal(result, os.Stderr)
}

func (s *LoggerSuite) TestConfigureLogDefault() {
	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	mockFileSystem.On("Stat", DefaultLoggingDir).Return(mockFileInfo{}, nil)

	result := builder.Build(LogOutputDefault, "")

	s.Equal(result, os.Stdout)
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
