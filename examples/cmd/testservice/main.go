package main

// Copyright (C) 2022 by Posit Software, PBC.

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
)

// Usage:
///
// To use this test service, simply start the application with:
// `./out/testservice`
//
// Then, send the HUP signal to the process and watch the output:
// `pkill -HUP testservice`

type leveler struct {
	level slog.Level
}

func (l *leveler) Level() slog.Level {
	return l.level
}

func main() {
	handler := randomHander()
	logger := slog.New(handler)
	slog.SetDefault(logger)

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
			handler = randomHander()
			logger = slog.New(handler)
			slog.SetDefault(logger)
			slog.Info("Chose random level and format\n\n")

			// Log some stuff to prove that the randomly chosen level and format are honored.
			doLogs()
		}
	}
}

func randomLevel() slog.Level {
	levels := []slog.Level{
		slog.Level(-8),
		slog.LevelDebug,
		slog.LevelInfo,
		slog.LevelWarn,
		slog.LevelError,
	}
	return levels[rand.Intn(len(levels))]
}

func randomHander() slog.Handler {
	formats := []slog.Handler{
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &leveler{level: randomLevel()}}),
		slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &leveler{level: randomLevel()}}),
	}
	return formats[rand.Intn(len(formats))]
}

func doLogs() {
	slog.Log(context.Background(), slog.Level(-8), "TRACE message.")
	slog.Debug("DEBUG message.")
	slog.Info("INFO message.")
	slog.Warn("WARN message.")
	slog.Error("ERROR message.")
	fmt.Printf("\n")
}
