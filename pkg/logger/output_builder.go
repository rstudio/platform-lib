package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

// Copyright (C) 2021 by RStudio, PBC.

const (
	DefaultFileLoggingPath   = "/var/log/rstudio/rstudio-connect/rstudio-connect.log"
	DefaultFileAccessLogPath = "/var/log/rstudio/rstudio-connect/rstudio-connect.access.log"
	DefaultFileAuditLogPath  = "/var/log/rstudio/rstudio-connect/rstudio-connect.audit.log"
	DefaultLoggingDir        = "/var/log/rstudio/rstudio-connect"
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

const (
	ServerLog LogCategory = "SERVER"
	AccessLog LogCategory = "ACCESS"
	AuditLog  LogCategory = "AUDIT"
)

func (c LogCategory) Text() string {
	text := fmt.Sprintf("%v Logs", c)
	return strings.Title(strings.ToLower(text))
}

func defaultFileByCategory(cty LogCategory) string {
	switch cty {
	case AccessLog:
		return DefaultFileAccessLogPath
	case AuditLog:
		return DefaultFileAuditLogPath
	default:
		return DefaultFileLoggingPath
	}
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
	Build(output LogOutputType, filepath string) io.Writer
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

func (b outputBuilder) Build(output LogOutputType, filepath string) io.Writer {
	switch output {
	case LogOutputDefault:
		return os.Stdout
	case LogOutputStdout:
		return os.Stdout
	case LogOutputStderr:
		return os.Stderr
	case LogOutputFile:
		return b.createLogFile(filepath)
	default:
		log.Printf("The output %q provided for the [Logging] Output configuration field isn't supported.", output)
		return b.fallbackOutput("")
	}
}

func (b outputBuilder) getDefaultLoggingPath() (io.Writer, error) {
	defaultFilepath := defaultFileByCategory(b.logCategory)
	return b.fileSystem.OpenFile(defaultFilepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
}

func (b outputBuilder) createDefaultLoggingDir() {
	if _, err := b.fileSystem.Stat(DefaultLoggingDir); os.IsNotExist(err) {
		if err := b.fileSystem.MkdirAll(DefaultLoggingDir, 0777); err != nil {
			log.Printf("Error when trying to create the default logging folder %q. %v", DefaultLoggingDir, err)
		}
	} else if err != nil {
		log.Printf("An error occurred trying to verify if the default logging folder exists. %v", err)
	}
}

func (b outputBuilder) fallbackOutput(filepath string) io.Writer {
	defaultFilepath := defaultFileByCategory(b.logCategory)
	log.Printf("Connect will use default logging file %q", defaultFilepath)
	writer, err := b.getDefaultLoggingPath()
	if err != nil {
		log.Printf(`Not possible to open or create the default logging file %q. Error: %v. Connect will use "stdout" for logging.`, defaultFilepath, err)
		writer = os.Stdout
	}
	return writer
}

func (b outputBuilder) createLogFile(filepath string) io.Writer {
	// we always create the default logging folder if it doesn´t exit yet
	b.createDefaultLoggingDir()

	if strings.TrimSpace(filepath) == "" {
		log.Printf("Logging.Output is set to FILE, but Logging.Path is empty.")
		return b.fallbackOutput(filepath)
	}

	log.Printf("Using file %s to store %s.", filepath, b.logCategory.Text())

	writer, err := b.fileSystem.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)

	if err != nil {
		log.Printf("Not possible to open or create the specified logging file %q. Error: %v.", filepath, err)
		return b.fallbackOutput(filepath)
	}

	return writer
}
