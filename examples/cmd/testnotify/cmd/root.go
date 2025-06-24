package cmd

// Copyright (C) 2022 by Posit Software, PBC.

import (
	"errors"
	"time"

	"github.com/lib/pq"
	"github.com/spf13/cobra"
)

const (
	defaultPgConnStr = "postgres://admin:password@postgres/postgres?sslmode=disable"
)

type testNotification struct {
	MessageType uint8
	Message     string
	UUID        string
}

func (t *testNotification) Type() uint8 {
	return t.MessageType
}

func (t *testNotification) Guid() string {
	return t.UUID
}

func (t *testNotification) Data() interface{} {
	return t.Message
}

var RootCmd = &cobra.Command{
	Use:   "testnotify",
	Short: "RStudio Go Libraries",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errors.New("Please choose a command.")
	},
}

type pqlistener struct {
	ConnStr string
}

func (p *pqlistener) NewListener() (*pq.Listener, error) {
	minReconn := 10 * time.Second
	maxReconn := time.Minute
	return pq.NewListener(p.ConnStr, minReconn, maxReconn, nil), nil
}

func (p *pqlistener) IP() (string, error) {
	return "", nil
}
