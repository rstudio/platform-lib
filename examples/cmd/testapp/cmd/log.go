package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"log"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/spf13/cobra"
)

var (
	message      string
	withMetadata bool
)

func init() {
	LogCmd.Example = `  testapp log --message=hello --level=INFO
`
	LogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")
	CaptureLogCmd.Flags().BoolVar(&withMetadata, "withMetadata", false, "The option to turn on or off the metadata in the capturing logger")

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
				WithMetadata: withMetadata,
			},
		)
		logs.Infof(message)
		logs.WithField("field", "value").Infof("Second Message, with argument: %d", 35)

		log.Printf("%v", logs.Messages())
		return nil
	},
}

var CompositeLogCmd = &cobra.Command{
	Use:     "composite",
	Short:   "Command to use the Composite logger",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		defaultLogger := rslog.DefaultLogger().WithField("Logger", "DefaultLogger")
		anotherLogger := rslog.DefaultLogger().WithField("Logger", "AnotherLogger")

		log := rslog.ComposeLoggers(defaultLogger, anotherLogger)

		log.Infof(message)
		return nil
	},
}
