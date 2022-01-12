package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/spf13/cobra"
)

var (
	message string
)

func init() {
	LogCmd.Example = `  rspm log --message=hello --level=INFO
`
	LogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")

	RootCmd.AddCommand(LogCmd)
}

var LogCmd = &cobra.Command{
	Use:     "log",
	Short:   "Command to log some information",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Log an info-level message
		rslog.Infof(message)
		return nil
	},
}
