package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/spf13/cobra"
)

func init() {
	DebugCmd.Example = `  testapp debug --message=hello --level=TRACE
`
	DebugCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")

	RootCmd.AddCommand(DebugCmd)
}

var DebugCmd = &cobra.Command{
	Use:     "debug",
	Short:   "Command to debug log some information",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		debugLogger := rslog.NewDebugLogger(TestDebug)
		// Log a debug-level debug message for the `TestDebug` product region.
		debugLogger.Debugf(fmt.Sprintf("Debug Message: %s", message))
		// Also log a trace-level debug message for the `TestDebug` product region.
		debugLogger.Tracef(fmt.Sprintf("Trace Message: %s", message))
		return nil
	},
}
