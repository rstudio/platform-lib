package rslog

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type LogOutputType string

const (
	LogOutputStdout LogOutputType = "STDOUT"
	LogOutputFile   LogOutputType = "FILE"
	LogOutputStderr LogOutputType = "STDERR"

	// We will use this output type only internally.
	// We need to know when the default option comes from an empty configuration
	// or when the user sets to our default option (stdout)
	LogOutputDefault LogOutputType = "DEFAULT"
)

type LogCategory string

func (c LogCategory) Text() string {
	text := fmt.Sprintf("%v Logs", c)
	return strings.Title(strings.ToLower(text))
}

func (t *LogOutputType) UnmarshalText(text []byte) (err error) {
	values := []LogOutputType{
		LogOutputStdout,
		LogOutputFile,
		LogOutputStderr,
	}

	for _, currentValue := range values {
		if strings.EqualFold(string(text), string(currentValue)) {
			*t = currentValue
			return nil
		}
	}

	return fmt.Errorf("invalid LogOutputType value '%s'. Allowed values are %v", text, values)
}

type FileSystem interface {
	Stat(path string) (os.FileInfo, error)
	MkdirAll(path string, perm os.FileMode) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
}

func NewOSFileSystem() FileSystem {
	return osFileSystem{}
}

type osFileSystem struct{}

func (o osFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (o osFileSystem) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

func (o osFileSystem) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

type OutputBuilder interface {
	Build(output LogOutputType, logFilePath string) (io.Writer, error)
}

type outputBuilder struct {
	logCategory LogCategory
	fileSystem  FileSystem
	defaultFile string
}

func NewOutputLogBuilder(logCategory LogCategory, defaultFile string) OutputBuilder {
	return outputBuilder{
		logCategory: logCategory,
		fileSystem:  NewOSFileSystem(),
		defaultFile: defaultFile,
	}
}

func (b outputBuilder) Build(output LogOutputType, logFilePath string) (io.Writer, error) {
	switch output {
	case LogOutputDefault:
		return os.Stderr, nil
	case LogOutputStdout:
		return os.Stdout, nil
	case LogOutputStderr:
		return os.Stderr, nil
	case LogOutputFile:
		return b.createLogFile(logFilePath)
	default:
		err := fmt.Errorf("The output %q provided for the logging output configuration field isn't supported.", output)
		return nil, err
	}
}

func (b outputBuilder) createLoggingDir(logFilePath string) {
	lgr := DefaultLogger()
	loggingDir := filepath.Dir(logFilePath)
	if _, err := b.fileSystem.Stat(loggingDir); os.IsNotExist(err) {
		// TODO: only the deepest directory needs to be 0777
		if err := b.fileSystem.MkdirAll(loggingDir, 0777); err != nil {
			lgr.Errorf("Error when trying to create the default logging directory %q. %v", loggingDir, err)
		}
	} else if err != nil {
		lgr.Errorf("An error occurred trying to verify if the default logging directory exists. %v", err)
	}
}

func (b outputBuilder) fallbackOutput() (io.Writer, error) {
	lgr := DefaultLogger()
	lgr.Infof("Attempting to use default logging file %q", b.defaultFile)
	writer, err := b.fileSystem.OpenFile(b.defaultFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		err = fmt.Errorf(`Not possible to open the default logging file %s. Error: %v.`, b.defaultFile, err)
		return nil, err
	}
	return writer, nil
}

func (b outputBuilder) createLogFile(logFilePath string) (io.Writer, error) {
	lgr := DefaultLogger()
	if strings.TrimSpace(logFilePath) == "" {
		lgr.Infof("Logging output is set to FILE, but no path was provided.")
		return b.fallbackOutput()
	}

	// create the provided logging directory if it doesn't exist
	b.createLoggingDir(logFilePath)

	lgr.Infof("Using file %s to store %s.", logFilePath, b.logCategory.Text())

	writer, err := b.fileSystem.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		lgr.Errorf("Not possible to open or create the specified logging file %q. Error: %v.", logFilePath, err)
		return b.fallbackOutput()
	}

	return writer, nil
}
