package main

// Copyright (C) 2022 by Posit Software, PBC.

import (
	"log"
	"log/slog"
	"os"

	"github.com/rstudio/platform-lib/v2/examples/cmd/testnotify/cmd"
)

type leveler struct {
	level slog.Level
}

func (l *leveler) Level() slog.Level {
	return l.level
}

func init() {
	log.SetFlags(0)
}

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: &leveler{level: slog.LevelDebug},
	}))
	slog.SetDefault(logger)

	cmd.RootCmd.SetOut(os.Stdout)
	cmd.RootCmd.SetErr(os.Stderr)
	// Each command is in the cmd subdirectory and the RootCmd houses
	// the global and inherited properties.
	err := cmd.RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
