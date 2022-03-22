package rslog

// Copyright (C) 2022 by RStudio, PBC.

import (
	"errors"
	"io"
	"os"

	"github.com/stretchr/testify/mock"
)

type ioWriterMock struct {
	mock.Mock
}

func (m *ioWriterMock) Write(p []byte) (int, error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (s *LoggerSuite) TestBuildStdout() {
	expectedWriter := &ioWriterMock{}

	mockFileSystem := &MockFileSystem{}
	mockFileSystem.On("Stdout").Return(expectedWriter)

	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	result, err := builder.Build(OutputDest{
		Output:   LogOutputStdout,
		Filepath: "",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestBuildEmptyFile() {
	expectedWriter := &ioWriterMock{}

	mockFileSystem := &MockFileSystem{}
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	mockFileSystem.On("Stat", "/default/log/logfile.log").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/default/log/logfile.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(expectedWriter, nil)

	result, err := builder.Build(OutputDest{
		Output:   LogOutputFile,
		Filepath: "",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestBuildEmptyFileFallbackErr() {

	mockFileSystem := &MockFileSystem{}
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	writerMock := &ioWriterMock{}

	mockFileSystem.On("Stat", "/default/log/logfile.log").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/default/log/logfile.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(writerMock, errors.New("Error"))

	_, err := builder.Build(OutputDest{
		Output:   LogOutputFile,
		Filepath: "",
	})

	s.NotNil(err)
}

func (s *LoggerSuite) TestBuildOpenFileError() {

	mockFileSystem := &MockFileSystem{}
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	expectedWriter := &ioWriterMock{}

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
	).Return(expectedWriter, nil)

	result, err := builder.Build(
		OutputDest{
			Output:   LogOutputFile,
			Filepath: "/custom/test.log",
		})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestConfigureLogFileOutput() {

	mockFileSystem := new(MockFileSystem)
	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	expectedWriter := &ioWriterMock{}

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(expectedWriter, nil)

	result, err := builder.Build(OutputDest{
		Output:   LogOutputFile,
		Filepath: "/custom/test.log",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestConfigureLogDefaultLoggingNonExistingDirectory() {
	mockFileSystem := &MockFileSystem{}

	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	expectedWriter := &ioWriterMock{}

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, os.ErrNotExist)
	mockFileSystem.On("MkdirAll", "/custom", os.FileMode(0777)).Return(nil)
	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(expectedWriter, nil)

	result, err := builder.Build(OutputDest{
		Output:   LogOutputFile,
		Filepath: "/custom/test.log",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestConfigureLogStderr() {
	mockFileSystem := &MockFileSystem{}
	expectedWriter := &ioWriterMock{}

	mockFileSystem.On("Stderr").Return(expectedWriter)
	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	result, err := builder.Build(OutputDest{
		Output:   LogOutputStderr,
		Filepath: "",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
}

func (s *LoggerSuite) TestConfigureLogDefault() {
	mockFileSystem := &MockFileSystem{}
	expectedWriter := &ioWriterMock{}

	mockFileSystem.On("Stderr").Return(expectedWriter)

	builder := outputBuilder{
		fileSystem: mockFileSystem,
	}

	result, err := builder.Build(OutputDest{
		Output:   LogOutputDefault,
		Filepath: "",
	})

	s.Nil(err)
	content := []byte("mock-content")
	expectedWriter.On("Write", content).Return(0, nil)

	result.Write(content)
	expectedWriter.AssertExpectations(s.T())
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

func (s *LoggerSuite) TestMultipleOutput() {
	mockFileSystem := &MockFileSystem{}

	builder := outputBuilder{
		fileSystem:  mockFileSystem,
		defaultFile: "/default/log/logfile.log",
	}

	fileExpectedWriter := &ioWriterMock{}
	stdoutExpectedWriter := &ioWriterMock{}
	stderrExpectedWriter := &ioWriterMock{}

	mockFileSystem.On("Stat", "/custom").Return(mockFileInfo{}, nil)
	mockFileSystem.On(
		"OpenFile",
		"/custom/test.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		os.FileMode(0600),
	).Return(fileExpectedWriter, nil)
	mockFileSystem.On("Stderr").Return(stderrExpectedWriter)
	mockFileSystem.On("Stdout").Return(stdoutExpectedWriter)

	outputDest := []OutputDest{
		{
			Output:   LogOutputFile,
			Filepath: "/custom/test.log",
		},
		{
			Output: LogOutputStdout,
		},
		{
			Output: LogOutputStderr,
		},
	}

	result, err := builder.Build(outputDest...)
	s.Nil(err)

	content := []byte("mock-content")
	fileExpectedWriter.On("Write", content).Return(len(content), nil)
	stderrExpectedWriter.On("Write", content).Return(len(content), nil)
	stdoutExpectedWriter.On("Write", content).Return(len(content), nil)

	result.Write(content)

	fileExpectedWriter.AssertExpectations(s.T())
	stderrExpectedWriter.AssertExpectations(s.T())
	stdoutExpectedWriter.AssertExpectations(s.T())
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
func (o *MockFileSystem) OpenFile(name string, flag int, perm os.FileMode) (io.Writer, error) {
	args := o.Called(name, flag, perm)
	return args.Get(0).(io.Writer), args.Error(1)
}

func (o *MockFileSystem) Stderr() io.Writer {
	args := o.Called()
	return args.Get(0).(io.Writer)
}

func (o *MockFileSystem) Stdout() io.Writer {
	args := o.Called()
	return args.Get(0).(io.Writer)
}

type mockFileInfo struct {
	// Embedding the interface like that allows to implement only the needed methods in the test functions
	os.FileInfo
}
