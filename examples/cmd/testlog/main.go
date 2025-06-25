package main

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"log"
	"os"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/rstudio/platform-lib/v2/examples/cmd/testlog/cmd"
)

func init() {
	log.SetFlags(0)
}

func main() {
	log.SetOutput(os.Stdout)
	cmd.RootCmd.SetOut(os.Stdout)
	cmd.RootCmd.SetErr(os.Stderr)

	// Since logs have been buffered (during root init), this will flush
	// all buffered logs after the program exits.
	defer rslog.Flush()

	// Each command is in the cmd subdirectory and the RootCmd houses
	// the global and inherited properties.
	err := cmd.RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
