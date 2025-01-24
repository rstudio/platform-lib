package rslog

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"sync"
)

// ProductRegion is a numerical value assigned to an area of the product.
type ProductRegion int

// debugMutex protects access to product debug region/enabled information.
var debugMutex sync.RWMutex

// debugRegions relates a numerical product debug region to its string
// equivalent. Used for display (when logging) and when processing
// configuration values.
//
// Protected by debugMutex.
var debugRegions map[ProductRegion]string = map[ProductRegion]string{}

// debugEnabled records product debug regions which enable debug logging.
//
// Protected by debugMutex.
var debugEnabled map[ProductRegion]bool = map[ProductRegion]bool{}

// RegisterRegions registers product debug regions. Called once at program
// startup.
func RegisterRegions(regions map[ProductRegion]string) {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	debugRegions = regions
}

// RegionNames returns the names of all registered product debug regions.
func RegionNames() []string {
	debugMutex.RLock()
	defer debugMutex.RUnlock()

	var names []string
	for _, name := range debugRegions {
		names = append(names, name)
	}
	return names
}

// Regions returns the numerical product debug regions.
func Regions() []ProductRegion {
	debugMutex.RLock()
	defer debugMutex.RUnlock()

	var regions []ProductRegion
	for r := range debugRegions {
		regions = append(regions, r)
	}
	return regions
}

// RegionByName returns the numerical product debug region associated with
// name. Returns zero when there is no match.
func RegionByName(name string) ProductRegion {
	debugMutex.RLock()
	defer debugMutex.RUnlock()

	for region, region_name := range debugRegions {
		if name == region_name {
			return region
		}
	}
	return 0
}

// RegionName returns the name associated with the numerical product debug
// region. Returns an empty string when there is no match.
func RegionName(region ProductRegion) string {
	debugMutex.RLock()
	defer debugMutex.RUnlock()

	return debugRegions[region]
}

// InitDebugLogs registers a set of product debug regions as enabled.
//
// This should be called as early as possible when starting an application,
// but after RegisterRegions.
func InitDebugLogs(regions []ProductRegion) {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	// Reset enabled regions on each call.
	debugEnabled = make(map[ProductRegion]bool)
	lgr := DefaultLogger()

	// match debug log region to list
	for _, region := range regions {
		if region == 0 {
			continue
		}
		debugEnabled[region] = true
		regionName := debugRegions[region]
		lgr.Infof("Debug logging enabled for area: %s", regionName)
	}
}

// Enable turns on debug logging for region. Useful in test.
func Enable(region ProductRegion) {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	debugEnabled[region] = true
}

// Disable turns on debug logging for region. Useful in test.
func Disable(region ProductRegion) {
	debugMutex.Lock()
	defer debugMutex.Unlock()

	debugEnabled[region] = false
}

// Enabled returns true if debug logging is configured for region.
func Enabled(region ProductRegion) bool {
	debugMutex.RLock()
	defer debugMutex.RUnlock()

	return debugEnabled[region]
}

type DebugLogger interface {
	Enabled() bool
	Debugf(msg string, args ...interface{})
	Tracef(msg string, args ...interface{})
	WithFields(fields Fields) DebugLogger
	WithSubRegion(subregion string) DebugLogger
}

type debugLogger struct {
	Logger
	region ProductRegion
}

// NewDebugLogger returns a new logger which includes the name of the product
// debug region at every message. Debugf and Tracef only occur when the
// product debug region is enabled.
//
// Logging occurs only when enabled for region.
func NewDebugLogger(region ProductRegion) *debugLogger {
	lgr := DefaultLogger()

	regionName := RegionName(region)
	entry := lgr.WithFields(Fields{
		"region": regionName,
	})

	dbglgr := &debugLogger{
		Logger: entry,
		region: region,
	}

	return dbglgr
}

// Enabled returns true if debug logging is enabled for the associated product
// debug region.
func (l *debugLogger) Enabled() bool {
	return Enabled(l.region)
}

// Debugf logs a message when debug logging is enabled for the associated
// product debug region.
func (l *debugLogger) Debugf(message string, args ...interface{}) {
	if l.Enabled() {
		l.Logger.Debugf(message, args...)
	}
}

// Tracef logs a message when debug logging is enabled for the associated
// product debug region.
func (l *debugLogger) Tracef(message string, args ...interface{}) {
	if l.Enabled() {
		l.Logger.Tracef(message, args...)
	}
}

// WithFields returns a new logger having additional fields. The returned
// logger is associated with the same product debug region.
func (l *debugLogger) WithFields(fields Fields) DebugLogger {
	newLgr := l.Logger.WithFields(fields)
	dbglgr := &debugLogger{
		Logger: newLgr,
		region: l.region,
	}
	return dbglgr
}

// WithSubRegion returns a debug logger having an additional "sub_region"
// field. The returned logger is associated with the same product debug
// region.
//
// Equivalent to `debugLogger.WithField("sub_region", subregion)`
func (l *debugLogger) WithSubRegion(subregion string) DebugLogger {
	newLgr := l.Logger.WithField("sub_region", subregion)
	dbglgr := &debugLogger{
		Logger: newLgr,
		region: l.region,
	}
	return dbglgr
}
