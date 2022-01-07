package main

// Copyright (C) 2021 by RStudio, PBC.

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/rstudio/platform-lib/pkg/rslog/debug"
)

// Usage:
///
// To use this test service, simply start the application with:
// `./out/testservice`
//
// Then, send the HUP signal to the process and watch the output:
// `pkill -HUP testservice`

type factory struct{}

// DefaultLogger is a factory method that provides the default logger for rslog.
func (f *factory) DefaultLogger() rslog.Logger {
	lgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
		Output: []rslog.OutputDest{{
			Output: rslog.LogOutputStdout,
		}},
		Format: rslog.JSONFormat,
		Level:  rslog.ErrorLevel,
	}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

	if err != nil {
		log.Fatalf("%v", err)
	}
	return lgr
}

var debugLogger debug.DebugLogger

const (
	RegionTest debug.ProductRegion = 1
)

func init() {
	// Replace the default factory so we can control the default logger.
	rslog.DefaultLoggerFactory = &factory{}

	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())

	// Initialize debug logging
	debug.InitLogs([]debug.ProductRegion{
		RegionTest,
	})
	debug.RegisterRegions(map[debug.ProductRegion]string{
		RegionTest: "test-debug",
	})
	debugLogger = debug.NewDebugLogger(RegionTest)
}

func main() {
	// Trap signals
	sigHUP := make(chan os.Signal, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigHUP, syscall.SIGHUP)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Print logs initially.
	doLogs()

	for {
		select {
		case <-sigCh:
			// Exit on a SIGTERM or SIGINT
			return
		case <-sigHUP:
			// When SIGHUP is received, choose a random log level and random format.
			level := randomLevel()
			format := randomFormat()
			fmt.Printf("Chose random level %s and format %s\n\n", level, format)

			// Update the default logger with the random level and format.
			err := rslog.UpdateDefaultLogger(rslog.LoggerOptionsImpl{
				Output: []rslog.OutputDest{{
					Output: rslog.LogOutputStdout,
				}},
				Level:  level,
				Format: format,
			}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))
			if err != nil {
				log.Panic(err)
			}

			// Log some stuff to prove that the randomly chosen level and format are honored.
			doLogs()
		}
	}
}

func randomLevel() rslog.LogLevel {
	levels := []rslog.LogLevel{
		rslog.TraceLevel,
		rslog.DebugLevel,
		rslog.InfoLevel,
		rslog.WarningLevel,
		rslog.ErrorLevel,
	}
	return levels[rand.Intn(len(levels))]
}

func randomFormat() rslog.OutputFormat {
	formats := []rslog.OutputFormat{
		rslog.TextFormat,
		rslog.JSONFormat,
	}
	return formats[rand.Intn(len(formats))]
}

func doLogs() {
	rslog.Tracef("TRACE message.")
	rslog.Debugf("DEBUG message.")
	rslog.Infof("INFO message.")
	rslog.Warnf("WARN message.")
	rslog.Errorf("ERROR message.")
	debugLogger.Debugf("Debug logger DEBUG message.")
	debugLogger.Tracef("Debug logger TRACE message.")
	fmt.Printf("\n")
}
