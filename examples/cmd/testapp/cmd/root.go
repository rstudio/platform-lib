package cmd

// Copyright (C) 2021 by RStudio, PBC.

import (
	"errors"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "testapp",
	Short: "RStudio Go Libraries",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errors.New("Please choose a command.")
	},
}
