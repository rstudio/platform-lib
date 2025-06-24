package rslogtest

// Copyright (C) 2022 by Posit, PBC.

import (
	"fmt"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestPackage(t *testing.T) {
	suite.Run(t, &MocksSuite{})
}

type MocksSuite struct {
	suite.Suite
}

func (s *MocksSuite) TestLoggerMockCallStack() {

	loggerMock := &LoggerMock{}
	loggerMock.AllowAny("Debugf", "Tracef", "Info", "Infof", "Warnf", "Errorf", "Fatal", "Fatalf", "Panicf")

	value := "the-value"

	s.Equal(0, len(loggerMock.Calls))

	loggerMock.Debugf("debugf '%s'", value)
	s.Equal("debugf 'the-value'", loggerMock.LastCall())
	s.Equal("debugf 'the-value'", loggerMock.Call(0))

	loggerMock.Tracef("tracef '%s'", value)
	s.Equal("tracef 'the-value'", loggerMock.LastCall())
	s.Equal("tracef 'the-value'", loggerMock.Call(1))

	loggerMock.Info("info")
	s.Equal("info", loggerMock.LastCall())
	s.Equal("info", loggerMock.Call(2))

	loggerMock.Infof("infof '%s'", value)
	s.Equal("infof 'the-value'", loggerMock.LastCall())
	s.Equal("infof 'the-value'", loggerMock.Call(3))

	loggerMock.Warnf("warnf '%s'", value)
	s.Equal("warnf 'the-value'", loggerMock.LastCall())
	s.Equal("warnf 'the-value'", loggerMock.Call(4))

	loggerMock.Errorf("errorf '%s'", value)
	s.Equal("errorf 'the-value'", loggerMock.LastCall())
	s.Equal("errorf 'the-value'", loggerMock.Call(5))

	// Sprint() does not add space between arguments when both are strings.
	loggerMock.Fatal("fatal", value)
	s.Equal("fatalthe-value", loggerMock.LastCall())
	s.Equal("fatalthe-value", loggerMock.Call(6))

	loggerMock.Fatalf("fatalf '%s'", value)
	s.Equal("fatalf 'the-value'", loggerMock.LastCall())
	s.Equal("fatalf 'the-value'", loggerMock.Call(7))

	loggerMock.Panicf("panicf '%s'", value)
	s.Equal("panicf 'the-value'", loggerMock.LastCall())
	s.Equal("panicf 'the-value'", loggerMock.Call(8))

	s.Equal(9, len(loggerMock.Calls))

	loggerMock.Clear()

	s.Equal(0, len(loggerMock.Calls))
}

// TestLoggerMockConcurrency helps confirm that LoggerMock can be used to mock
// logging across multiple goroutines. Run with: go test -race.
func (s *MocksSuite) TestLoggerMockConcurrency() {
	logger := &LoggerMock{}
	logger.AllowAny("Infof")

	const workers = 10
	const messages = 20

	// ready pauses the goroutines so they all start producing messages
	// together.
	ready := make(chan interface{})

	wg := &sync.WaitGroup{}
	wg.Add(workers)

	produce := func(id int) {
		<-ready
		for i := 0; i < messages; i++ {
			logger.Infof("message %d from %d", i, id)
		}
		wg.Done()
	}

	for i := 0; i < workers; i++ {
		go produce(i)
	}

	close(ready) // ready, set, go!
	wg.Wait()    // wait for all workers to finish producing.

	s.Equal(workers*messages, len(logger.Calls))

	// Check that the final message was formatted with numeric
	// information. We don't know the identifier for that producer.
	s.Regexp(
		regexp.MustCompile(fmt.Sprintf(`message %d from \d+`, (messages-1))),
		logger.Call((workers*messages)-1))
}
