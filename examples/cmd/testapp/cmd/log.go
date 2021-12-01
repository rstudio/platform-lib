package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"log"

	"github.com/rstudio/platform-lib/pkg/logger"
	"github.com/spf13/cobra"
)

var (
	message string
)

func init() {
	lgr, err := logger.NewLoggerImpl(logger.LoggerOptionsImpl{
		Output: []logger.OutputDest{
			{
				Output: logger.LogOutputStdout,
			},
		},
		Format: logger.JSONFormat,
		Level:  logger.DebugLevel,
	}, logger.NewOutputLogBuilder(logger.ServerLog, ""))

	if err != nil {
		log.Fatalf("%w", err)
	}

	logger.SetDefaultLogger(lgr)
	LogCmd.Example = `  rspm log --message=hello
`
	LogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")

	RootCmd.AddCommand(LogCmd)
}

var LogCmd = &cobra.Command{
	Use:     "log",
	Short:   "Command to log some information",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		logs := logger.DefaultLogger()
		logs.Infof(message)
		return nil
	},
}
