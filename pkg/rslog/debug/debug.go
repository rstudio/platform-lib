package debug

// Copyright (C) 2021 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/rslog"
)

type ProductRegion int

// regionNames translates the enum region to its string equivalent. This is
// used for display (when logging) and also when processing the configuration
// values.
var regionNames map[ProductRegion]string

type callbacksArr []func(flag bool)

var regionCallbacks = map[ProductRegion]callbacksArr{}
var regionsEnabled = map[ProductRegion]bool{}

func RegisterRegions(regions map[ProductRegion]string) {
	regionNames = regions
}

func RegionNames() []string {
	var names []string
	for _, name := range regionNames {
		names = append(names, name)
	}
	return names
}

func Regions() []ProductRegion {
	var regions []ProductRegion
	for r := range regionNames {
		regions = append(regions, r)
	}
	return regions
}

func RegionByName(text string) ProductRegion {
	for region, name := range regionNames {
		if name == text {
			return region
		}
	}
	return 0
}

func RegionName(region ProductRegion) string {
	return regionNames[region]
}

// Register debug regions enabled.
// This should be called as early as possible when starting an application.
func InitLogs(regions []ProductRegion) {
	// Reset enabled regions on each call.
	regionsEnabled = make(map[ProductRegion]bool)

	// match debug log region to list
	for _, region := range regions {
		if region == 0 {
			continue
		}
		regionsEnabled[region] = true
		// TODO: On logging feature completion,
		// Use the below commented out lines when
		// debug.InitLog at config.go isn't used anymore

		// regionName := DebugRegionName(region)
		// if we normalized, print both the enabled region and
		// the original input.
		// Infof("Debug logging enabled for area: %s", regionName)
	}
}

// Enable turns on logging for a named region. Useful in test.
func Enable(region ProductRegion) {
	regionsEnabled[region] = true
	_, ok := regionCallbacks[region]
	if ok {
		for _, cb := range regionCallbacks[region] {
			cb(true)
		}
	}
}

// Disable turns on logging for a named region. Useful in test.
func Disable(region ProductRegion) {
	regionsEnabled[region] = false
	_, ok := regionCallbacks[region]
	if ok {
		for _, cb := range regionCallbacks[region] {
			cb(false)
		}
	}
}

// Enabled returns true if debug logging is configured for this region.
func Enabled(region ProductRegion) bool {
	return regionsEnabled[region]
}

type DebugLogger interface {
	Enabled() bool
	Debugf(msg string, args ...interface{})
	Tracef(msg string, args ...interface{})
	WithFields(fields rslog.Fields) DebugLogger
	WithSubRegion(subregion string) DebugLogger
}

type debugLogger struct {
	rslog.Logger
	region  ProductRegion
	enabled bool
}

// NewDebugLogger returns a new logger which includes
// the name of the debug region at every message.
func NewDebugLogger(region ProductRegion) *debugLogger {
	lgr := rslog.DefaultLogger()

	entry := lgr.WithFields(rslog.Fields{
		"region": regionNames[region],
	})

	dbglgr := &debugLogger{
		Logger:  entry,
		region:  region,
		enabled: Enabled(region),
	}

	registerLoggerCb(region, dbglgr.enable)

	return dbglgr
}

func registerLoggerCb(region ProductRegion, cb func(bool)) {
	regionCallbacks[region] = append(regionCallbacks[region], cb)
}

// Enabled returns true if debug logging is enabled for this rslog.
func (l *debugLogger) Enabled() bool {
	return Enabled(l.region)
}

func (l *debugLogger) Debugf(message string, args ...interface{}) {
	if l.enabled {
		l.Logger.Debugf(message, args...)
	}
}

func (l *debugLogger) Tracef(message string, args ...interface{}) {
	if l.enabled {
		l.Logger.Tracef(message, args...)
	}
}

// Set fields to be logged
func (l *debugLogger) WithFields(fields rslog.Fields) DebugLogger {
	newLgr := l.Logger.WithFields(fields)
	dbglgr := &debugLogger{
		Logger: newLgr,
		region: l.region,
	}
	registerLoggerCb(l.region, dbglgr.enable)
	return dbglgr
}

// WithSubRegion returns a debug logger with further specificity
// via sub_region key:value. E.g "region": "LDAP", "sub_region": "membership scanner"
func (l *debugLogger) WithSubRegion(subregion string) DebugLogger {
	newLgr := l.Logger.WithField("sub_region", subregion)
	dbglgr := &debugLogger{
		Logger: newLgr,
		region: l.region,
	}
	registerLoggerCb(l.region, dbglgr.enable)
	return dbglgr
}

// Enable or disable this region debug logging instance
func (l *debugLogger) enable(enabled bool) {
	l.enabled = enabled
}
