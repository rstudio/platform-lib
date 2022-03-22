package main

// Copyright (C) 2022 by RStudio, PBC.

import (
	"log"
	"os"

	"github.com/rstudio/platform-lib/examples/cmd/testlog/cmd"
)

func init() {
	log.SetFlags(0)
}

func main() {
	log.SetOutput(os.Stdout)
	cmd.RootCmd.SetOut(os.Stdout)
	cmd.RootCmd.SetErr(os.Stderr)
	// Each command is in the cmd subdirectory and the RootCmd houses
	// the global and inherited properties.
	err := cmd.RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
