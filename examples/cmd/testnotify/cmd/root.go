package cmd

// Copyright (C) 2022 by RStudio, PBC.

import (
	"errors"
	"time"

	"github.com/lib/pq"
	"github.com/spf13/cobra"

	"github.com/rstudio/platform-lib/pkg/rslog"
)

const (
	defaultPgConnStr = "postgres://admin:password@postgres/postgres?sslmode=disable"

	RegionNotify rslog.ProductRegion = 1
)

func init() {

	// Initialize debug logging
	rslog.InitLogs([]rslog.ProductRegion{
		RegionNotify,
	})
	rslog.RegisterRegions(map[rslog.ProductRegion]string{
		RegionNotify: "test-notifications",
	})
}

type testNotification struct {
	Message string
	UUID    string
}

func (t *testNotification) Type() uint8 {
	return 1
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
