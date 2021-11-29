package logger

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"io"
	"log"
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
	Build(output LogOutputType, logFilePath, defaultLogFilePath string) io.Writer
}

type outputBuilder struct {
	logCategory LogCategory
	fileSystem  FileSystem
}

func NewOutputLogBuilder(logCategory LogCategory) OutputBuilder {
	return outputBuilder{
		logCategory: logCategory,
		fileSystem:  NewOSFileSystem(),
	}
}

func (b outputBuilder) Build(output LogOutputType, logFilePath, defaultLogFilePath string) io.Writer {
	switch output {
	case LogOutputDefault:
		return os.Stdout
	case LogOutputStdout:
		return os.Stdout
	case LogOutputStderr:
		return os.Stderr
	case LogOutputFile:
		return b.createLogFile(logFilePath, defaultLogFilePath)
	default:
		log.Printf("The output %q provided for the logging output configuration field isn't supported.", output)
		return b.fallbackOutput(defaultLogFilePath)
	}
}

func (b outputBuilder) createLoggingDir(logFilePath string) {
	loggingDir := filepath.Dir(logFilePath)
	if _, err := b.fileSystem.Stat(loggingDir); os.IsNotExist(err) {
		if err := b.fileSystem.MkdirAll(loggingDir, 0777); err != nil {
			log.Printf("Error when trying to create the default logging folder %q. %v", loggingDir, err)
		}
	} else if err != nil {
		log.Printf("An error occurred trying to verify if the default logging folder exists. %v", err)
	}
}

func (b outputBuilder) fallbackOutput(defaultLogFilePath string) io.Writer {
	log.Printf("Attempting to use default logging file %q", defaultLogFilePath)
	writer, err := b.fileSystem.OpenFile(defaultLogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Printf(`Not possible to open or create the default logging file %s. Using STDOUT for logging. Error: %v.`, defaultLogFilePath, err)
		writer = os.Stdout
	}
	return writer
}

func (b outputBuilder) createLogFile(logFilePath, defaultLogFilePath string) io.Writer {
	if strings.TrimSpace(logFilePath) == "" {
		log.Printf("Logging output is set to FILE, but no path was provided.")
		return b.fallbackOutput(defaultLogFilePath)
	}

	// we always create the default logging folder if it doesn't exit yet
	b.createLoggingDir(logFilePath)

	log.Printf("Using file %s to store %s.", logFilePath, b.logCategory.Text())

	writer, err := b.fileSystem.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		log.Printf("Not possible to open or create the specified logging file %q. Error: %v.", logFilePath, err)
		b.createLoggingDir(defaultLogFilePath)
		return b.fallbackOutput(defaultLogFilePath)
	}

	return writer
}
