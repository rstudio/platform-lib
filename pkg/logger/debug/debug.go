package debug

// Copyright (C) 2021 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/pkg/logger"
)

type ProductRegion int

const (
	Nothing            = ProductRegion(iota)
	Config             // Configuration
	AppVisibility      // API: app-list pagination
	OAuth2             // Auth: Google OAuth2
	LDAP               // Auth: LDAP
	PAM                // Auth: PAM
	ProxyAuth          // Auth: Proxied auth
	SAML               // Auth: SAML
	Services           // Service & filter routing
	Session            // Login session handling
	Proxy              // Shiny request proxying (HTTP & SockJS)
	ProxyTrace         // Overly verbose proxy tracing (low-level HTTP request tracing)
	RProc              // R process execution
	Reports            // Report rendering
	Recipients         // Recipient enumeration
	Worker             // Shiny workers
	WorkerEvents       // Shiny worker event tracing
	Router             // API request routing
	Licensing          // Licensing
	MailSender         // Trace mail sending
	Monitor            // Logging about metrics daemon
	BaseURLRedirect    // Trace root redirects
	URLNormalization   // Trace URL normalization filter rewriting and redirecting
	AppReducer         // Trace per-app metrics reducers
	PrometheusReducer  // Trace per-app prometheus metrics reducers
	JobSpool           // Trace processing of events in the job spool directory
	Queue              // Trace processing of items through the queue
	SQLiteBackups      // SQLite automated backup logging
	TFProc             // TensorFlow process execution
	PyProc             // Python process execution
	GitProc            // Git process execution
	Cleanup            // Cleanup/Janitor processes
	GracefulServer     // Graceful server management
	JobSynchronization // Job Synchronization
	JobReaper          // Job Reaping
	LauncherJobs       // Launcher job details
	JobFinalizer       // Orphaned job finalization
	TableauIntegration // Tableau Analytics Extensions API integration
)

// regionNames translates the enum region to its string equivalent. This is
// used for display (when logging) and also when processing the configuration
// values.
var regionNames = map[ProductRegion]string{
	AppVisibility:      "app-visibility",
	Config:             "config",
	OAuth2:             "oauth2",
	Services:           "services",
	Session:            "session",
	RProc:              "rproc",
	Reports:            "reports",
	Recipients:         "recipients",
	Worker:             "worker",
	WorkerEvents:       "worker.events",
	Proxy:              "proxy",
	ProxyTrace:         "proxy-trace",
	Router:             "router",
	Licensing:          "licensing",
	LDAP:               "ldap",
	PAM:                "pam",
	SAML:               "saml",
	ProxyAuth:          "proxy-auth",
	MailSender:         "mail-sender",
	Monitor:            "monitor",
	BaseURLRedirect:    "base-url-redirect",
	URLNormalization:   "url-normalization",
	AppReducer:         "app-reducer",
	PrometheusReducer:  "prometheus-reducer",
	JobSpool:           "job-spool",
	Queue:              "queue",
	SQLiteBackups:      "sqlite-backups",
	TFProc:             "tfproc",
	PyProc:             "pyproc",
	GitProc:            "gitproc",
	Cleanup:            "cleanup",
	GracefulServer:     "graceful-server",
	JobSynchronization: "job-sync",
	JobReaper:          "job-reaper",
	LauncherJobs:       "launcher-jobs",
	JobFinalizer:       "job-finalizer",
	TableauIntegration: "tableau-integration",
}

type callbacksArr []func(flag bool)

var regionCallbacks map[ProductRegion]callbacksArr
var regionsEnabled map[ProductRegion]bool

func init() {
	initRegions()
}

func initRegions() {
	regionCallbacks = make(map[ProductRegion]callbacksArr)
	regionsEnabled = make(map[ProductRegion]bool)
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
	return Nothing
}

func RegionName(region ProductRegion) string {
	return regionNames[region]
}

// Register debug regions enabled.
// This should be called as early as possible when starting up Connect.
func InitLogs(regions []ProductRegion) {
	// Reset enabled regions on each call.
	regionsEnabled = make(map[ProductRegion]bool)

	// match debug log region to list
	for _, region := range regions {
		if region == Nothing {
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

// Wrapper instances of RSCLogger for debugging purposes
type DebugLogger interface {
	Enabled() bool
	Debugf(msg string, args ...interface{})
	WithFields(fields logger.Fields) DebugLogger
	WithSubRegion(subregion string) DebugLogger
}

type debugLogger struct {
	logger.Entry
	lgr    logger.Entry
	region ProductRegion
}

// NewDebugLogger returns a new logger which includes
// the name of the debug region at every message.
func NewDebugLogger(region ProductRegion) DebugLogger {
	lgr := logger.DefaultLogger().Copy()

	if Enabled(region) {
		lgr.SetLevel(logger.DebugLevel)
		lgr.SetReportCaller(true)
	}

	entry := lgr.WithFields(logger.Fields{
		"region": regionNames[region],
	})

	dbglgr := &debugLogger{
		Entry:  entry,
		lgr:    entry,
		region: region,
	}

	registerLoggerCb(region, dbglgr.enable)

	return dbglgr
}

func registerLoggerCb(region ProductRegion, cb func(bool)) {
	regionCallbacks[region] = append(regionCallbacks[region], cb)
}

// Enabled returns true if debug logging is enabled for this logger.
func (l *debugLogger) Enabled() bool {
	return Enabled(l.region)
}

// Set fields to be logged
func (l *debugLogger) WithFields(fields logger.Fields) DebugLogger {
	newLgr := l.lgr.WithFields(fields)
	dbglgr := &debugLogger{
		Entry:  newLgr,
		lgr:    newLgr,
		region: l.region,
	}
	registerLoggerCb(l.region, dbglgr.enable)
	return dbglgr
}

// WithSubRegion returns a debug logger with further specificity
// via sub_region key:value. E.g "region": "LDAP", "sub_region": "membership scanner"
func (l *debugLogger) WithSubRegion(subregion string) DebugLogger {
	newLgr := l.lgr.WithField("sub_region", subregion)
	dbglgr := &debugLogger{
		Entry:  newLgr,
		lgr:    newLgr,
		region: l.region,
	}
	registerLoggerCb(l.region, dbglgr.enable)
	return dbglgr
}

// Enable or disable this region debug logging instance across Connect
func (l *debugLogger) enable(enabled bool) {
	if enabled {
		l.lgr.SetLevel(logger.DebugLevel)
	} else {
		l.lgr.SetLevel(logger.ErrorLevel)
	}
	l.lgr.SetReportCaller(enabled)
}
