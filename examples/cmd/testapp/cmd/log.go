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
	LogCmd.Example = `  rspm log --message=hello --level=INFO
`
	LogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")

	RootCmd.AddCommand(LogCmd, CaptureLogCmd, CompositeLogCmd)
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

var CaptureLogCmd = &cobra.Command{
	Use:     "capture",
	Short:   "Command to use the Capture logger",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		logs := rslog.NewCapturingLogger(
			rslog.CapturingLoggerOptions{
				Level:        rslog.InfoLevel,
				WithMetadata: false,
			},
		)
		logs.Infof(message)
		logs.Infof("Second Message, with argument: %d", 35)

		log.Printf("%v", logs.Messages())
		return nil
	},
}

var CompositeLogCmd = &cobra.Command{
	Use:     "composite",
	Short:   "Command to use the Capture logger",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		defualtLogger := rslog.DefaultLogger().WithField("Logger", "DefaultLogger")
		anotherLogger := rslog.DefaultLogger().WithField("Logger", "AnotherLogger")

		log := rslog.NewCompositeLogger([]rslog.Logger{defualtLogger, anotherLogger})

		log.Infof(message)
		return nil
	},
}
