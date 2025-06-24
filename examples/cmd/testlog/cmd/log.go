package cmd

// Copyright (C) 2025 by Posit Software, PBC.

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
	LogCmd.Example = `  testlog log --message=hello --level=INFO
`
	LogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")
	BufferedLogCmd.Flags().StringVar(&message, "message", "default message", "The message to log.")
	CaptureLogCmd.Flags().BoolVar(&withMetadata, "withMetadata", false, "The option to turn on or off the metadata in the capturing logger")

	RootCmd.AddCommand(LogCmd, TerminalLogCmd, CaptureLogCmd, CompositeLogCmd, BufferedLogCmd)
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

var TerminalLogCmd = &cobra.Command{
	Use:     "terminal-log",
	Short:   "Command to log some information using the terminal logging style",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		rslog.UseTerminalLogger(rslog.InfoLevel)
		rslog.WithField("some-field", "some-value").Infof(message)
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

var BufferedLogCmd = &cobra.Command{
	Use:     "buffered",
	Short:   "Command to use the Default logger with buffering functionality",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		defaultLogger := rslog.DefaultLogger()

		log := defaultLogger.WithField("Logger", "DefaultLogger")
		log.Warnf(message)

		// Since logs are only flushed after the command's execution, setting a new log level
		// before that will make flushed logs respect the newly set level, even if the log call
		// happened before setting the new level (works with output too).
		log.SetLevel(rslog.WarningLevel)

		return nil
	},
}
