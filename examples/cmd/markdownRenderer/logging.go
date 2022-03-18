package main

// Copyright (C) 2021 by RStudio, PBC.

import (
	"log"

	"github.com/rstudio/platform-lib/pkg/rslog"
)

// Define debug regions for the product.

const (
	RegionRenderer      rslog.ProductRegion = 1
	RegionStorage       rslog.ProductRegion = 2
	RegionNotifications rslog.ProductRegion = 3
	RegionQueue         rslog.ProductRegion = 4
	RegionCache         rslog.ProductRegion = 5
	RegionCacheNfsTime  rslog.ProductRegion = 6
	RegionAgent         rslog.ProductRegion = 7
	RegionAgentTrace    rslog.ProductRegion = 8
	RegionAgentJob      rslog.ProductRegion = 9
	RegionStore         rslog.ProductRegion = 10
)

type factory struct{}

// DefaultLogger is a factory method that provides the default logger for rslog.
func (f *factory) DefaultLogger() rslog.Logger {
	lgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
		Output: []rslog.OutputDest{{
			Output: rslog.LogOutputStdout,
		}},
		Format: rslog.TextFormat,
		Level:  rslog.DebugLevel,
	}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

	if err != nil {
		log.Fatalf("%v", err)
	}
	return lgr
}

func init() {
	// Replace the default factory to control the default logger.
	rslog.DefaultLoggerFactory = &factory{}

	// Initialize debug logging for all regions.
	rslog.InitDebugLogs([]rslog.ProductRegion{
		RegionRenderer,
		RegionStorage,
		RegionNotifications,
		RegionQueue,
		RegionCache,
		RegionAgent,
		RegionAgentTrace,
		RegionAgentJob,
		RegionStore,
	})
	// Register debug regions.
	rslog.RegisterRegions(map[rslog.ProductRegion]string{
		RegionRenderer:      "renderer",
		RegionStorage:       "storage",
		RegionNotifications: "notifications",
		RegionQueue:         "queue",
		RegionCache:         "cache",
		RegionAgent:         "agent",
		RegionAgentTrace:    "agent-trace",
		RegionAgentJob:      "agent-job",
		RegionStore:         "store",
	})
}
