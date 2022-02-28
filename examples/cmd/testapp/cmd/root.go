package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"errors"
	"log"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/spf13/cobra"
)

type factory struct{}

// DefaultLogger is a factory method that provides the default logger for rslog.
func (f *factory) DefaultLogger() rslog.Logger {
	lgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
		Output: []rslog.OutputDest{
			{
				Output: rslog.LogOutputStdout,
			},
		},
		Format: rslog.JSONFormat,
		Level:  rslog.LogLevel(level),
	}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

	if err != nil {
		log.Fatalf("%v", err)
	}
	return lgr
}

func (f *factory) TerminalLogger(l rslog.LogLevel) rslog.Logger {
	lgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
		Level: l,
	}, rslog.StderrOutputBuilder{})

	if err != nil {
		log.Fatalf("%v", err)
	}
	return lgr
}

const (
	// TestDebug is an example product region for debug logging
	TestDebug rslog.ProductRegion = 1
)

var (
	// level value is provided by the persistent (global) flag `--level`.
	level string
)

func init() {
	// Register product debug regions and initialize debug logging
	rslog.RegisterRegions(map[rslog.ProductRegion]string{
		TestDebug: "test-debug",
	})
	rslog.InitDebugLogs([]rslog.ProductRegion{
		TestDebug,
	})

	// Install the default logger factory. If this is not set, then `rslog` will create
	// its own default logger factory.
	rslog.DefaultLoggerFactory = &factory{}

	// The `--level` flag is available for all commands.
	RootCmd.PersistentFlags().StringVar(&level, "level", "DEBUG", "The log level.")
}

var RootCmd = &cobra.Command{
	Use:   "testapp",
	Short: "RStudio Go Libraries",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errors.New("Please choose a command.")
	},
}
