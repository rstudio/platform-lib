# `/pkg/rslog`

## Description

A logging library that meets the requirements of the [Posit logging standards](
https://rstudiopbc.atlassian.net/wiki/spaces/ENG/pages/36048281/RStudio+Logging+Standard).
Provides a logger that wraps `github.com/sirupsen/logrus`.

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
	Filename:   "/var/log/rstudio/myapp/server.log",
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
	Filename:   "/var/log/rstudio/myapp/server.log",
	MaxSize:    500, // megabytes
	MaxAge:     30, //days
	Compress:   true,
})

compLogger := rslog.ComposeLoggers(stdoutLgr, fileLgr)
```

This `compLogger` loops over its collection of loggers, sending the same messages to each. You can pass around this
`compLogger` as you would any other logger that implements the `rslog.Logger` interface. If you would like to obtain
this same logger in multiple files, you need to change the rslog default logger with a factory:

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

logger := rslog.DefaultLogger() // this will now get the same composite logger everywhere
logger.Infof("xyz")
```
