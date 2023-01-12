# `/pkg/rslog`

## Description

A logging library that meets the requirements of the Posit logging standards.
Provides a logger that wraps `github.com/sirupsen/logrus`.

### The Posit logging standards

Excerpts here come from the internal confluence space "RStudio Logging Standard"

* Configuration via environment variables (that override values in the configuration file) is encouraged.
* The output of logging should be file/syslog or stderr.  You can also support logging to file and syslog separately.
  Kubernetes expects logging to stdout/stderr and can have limited local storage that can be broken by file logging.
* File/syslog logging must always be supported and be the default type. Files should go in a location that is shared
  by all products.
* Output to stderr must be supported, including from child processes where possible. Program output where needed will
  go to stdout, while log messages will go to stderr.
* There should be a single log file per service-child process logs should be read into the parent to be put into the
  single log file. The log file should be clearly named for the service. Audit logs can be in a separate file, given
  that customers often want to ingest these files differently.
* The file must rotate when needed. This does not need to happen by the product itself, but if this depends on
  logrotate, this should be documented with a default config file. Rotation can be triggered by time or size. The old
  log file should be stored alongside the original with a numeric extension, .1, .2, etc. Logs should be removed after
  30 days
* The level of logging must be configurable. There must be the ability to turn debug logging on for all or parts of the
  application.
* To make the debug logging most useful, try to allow for debug logging to be enabled without a restart. This should be
  done by listening for SIGHUP to reload configurations. This should work with subprocesses as well, by sending the
  child process the same SIGHUP signal.
* JSON should be the default format for log data, with one line of JSON per log message. This maximizes compatibility
  with external log processing tools.

## Examples

- [markdownRenderer](../../examples/cmd/markdownRenderer/README.md) 
  demonstrates how to use `rslog` for debug logging.
- [testlog](../../examples/cmd/testlog) and
  [testservice](../../examples/cmd/testservice) also
  demonstrate using `rslog` for logging and debug logging.

### Using and changing the default logger

`rslog.NewDefaultLogger()` returns a singleton instance of a logger that can be used across multiple packages in your
application. There are setter methods on the logger that can be used to change the default logger's behavior. These
examples assume that you have `logger := rslog.NewDefaultLogger()`.

```go
// Set the default logger to use the JSON formatter
logger.SetFormatter(rslog.JSONFormat)
// Set the output to a file instead of stdout
logger.SetOutput(os.OpenFile("mylogfile.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666))
```

Remember that this is a singleton instance. Changes you make in one file will affect the logger in other files.

### Rotating log files

Rotating log files is not specified as part of the Posit logging standards to be part of an application.
It can be left to the host for utilities such as logrotate. However, should you prefer to implement it in
your application, the way to do it is to implement a file writer that does the rotation. One way to achieve
this is with the [lumberjack library](https://github.com/natefinch/lumberjack).

```go
logger := rslog.NewDefaultLogger()
logger.SetOutput(&lumberjack.Logger{
	Filename:   "/var/log/myorg/myapp/server.log",
	MaxSize:    500, // megabytes
	MaxAge:     30, //days
	Compress:   true,
})
```

### Multiple loggers / Teeing output to stdout and a file

For multiple loggers, you can use the `rslog.ComposeLoggers()` function. However, you must create loggers using methods
other than `rslog.NewDefaultLogger()`. Remember, that is a singleton instance, so each time you call that function, you
are getting the same logger, and any changes to it will affect earlier logger(s).

```go
stdoutLgr := rslog.DefaultLogger()
stdoutLgr.SetLevel(rslog.DebugLevel)

fileLgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
    Format: rslog.JSONFormat,
    Level:  rslog.DebugLevel,
}, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))
fileLgr.SetOutput(&lumberjack.Logger{
	Filename:   "/var/log/myorg/myapp/server.log",
	MaxSize:    500, // megabytes
	MaxAge:     30, //days
	Compress:   true,
})

compLogger := rslog.ComposeLoggers(stdoutLgr, fileLgr)
```

This `compLogger` loops over its collection of loggers, sending the same messages to each. You can pass around this
`compLogger` as you would any other logger that implements the `rslog.Logger` interface. If it is not convenient to pass
this object around, you can instead change the rslog.DefaultLogger factory. With this you can obtain the singleton instance
by calling rslog.DefaultLogger() without passing the instance around.

```go
type compositeFactory struct{}

func (f *compositeFactory) DefaultLogger() rslog.Logger {
  // Careful here: obviously we can't use rslog.DefaultLogger() here, since we are redefining it
  stdoutLgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
    //...
  }, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

  fileLgr, err := rslog.NewLoggerImpl(rslog.LoggerOptionsImpl{
    //...
  }, rslog.NewOutputLogBuilder(rslog.ServerLog, ""))

  compLogger := rslog.ComposeLoggers(stdoutLgr, fileLgr)
  return compLogger
}

rslog.DefaultLoggerFactory = &compositeFactory{}

logger := rslog.DefaultLogger() // this will now get the same composite logger instance everywhere
logger.Infof("xyz")
```
