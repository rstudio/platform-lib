package main

/* main.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

import (
	"log"
	"os"

	"github.com/rstudio/platform-lib/examples/cmd/testapp/cmd"
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
