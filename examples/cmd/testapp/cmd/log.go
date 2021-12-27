package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"log"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/spf13/cobra"
)

var (
	message string
)

func init() {
	lgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
		Output: []rslog.OutputDest{
			{
				Output: rslog.LogOutputStdout,
			},
		},
		Format: rslog.JSONFormat,
		Level:  rslog.DebugLevel,
	}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

	if err != nil {
		log.Fatalf("%v", err)
	}

	rslog.SetDefaultLogger(lgr)
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
		logs := rslog.DefaultLogger()
		logs.Infof(message)
		return nil
	},
}
