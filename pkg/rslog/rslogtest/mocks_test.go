package rslogtest

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestPackage(t *testing.T) {
	suite.Run(t, &MocksSuite{})
}

type MocksSuite struct {
	suite.Suite
}

func (s *MocksSuite) TestLoggerMockCallStack() {

	loggerMock := &LoggerMock{}

	debugfStr := "Debugf"
	tracefStr := "Tracef"
	infoStr := "Info"
	infofStr := "Infof"
	warnfStr := "Warnf"
	errorfStr := "Errorf"
	fatalfStr := "Fatalf"
	fatalStr := "Fatal"
	panicfStr := "Panicf"

	loggerMock.AllowAny(debugfStr, tracefStr, infoStr, infofStr, warnfStr, errorfStr, fatalfStr, fatalStr, panicfStr)

	s.Equal(0, len(loggerMock.Calls))

	loggerMock.Debugf(debugfStr)
	s.Equal(debugfStr, loggerMock.LastCall())
	s.Equal(debugfStr, loggerMock.Call(0))

	loggerMock.Tracef(tracefStr)
	s.Equal(tracefStr, loggerMock.LastCall())
	s.Equal(tracefStr, loggerMock.Call(1))

	loggerMock.Info(infoStr)
	s.Equal(infoStr, loggerMock.LastCall())
	s.Equal(infoStr, loggerMock.Call(2))

	loggerMock.Infof(infofStr)
	s.Equal(infofStr, loggerMock.LastCall())
	s.Equal(infofStr, loggerMock.Call(3))

	loggerMock.Warnf(warnfStr)
	s.Equal(warnfStr, loggerMock.LastCall())
	s.Equal(warnfStr, loggerMock.Call(4))

	loggerMock.Errorf(errorfStr)
	s.Equal(errorfStr, loggerMock.LastCall())
	s.Equal(errorfStr, loggerMock.Call(5))

	loggerMock.Fatalf(fatalfStr)
	s.Equal(fatalfStr, loggerMock.LastCall())
	s.Equal(fatalfStr, loggerMock.Call(6))

	loggerMock.Fatal(fatalStr)
	s.Equal(fatalStr, loggerMock.LastCall())
	s.Equal(fatalStr, loggerMock.Call(7))

	loggerMock.Panicf(panicfStr)
	s.Equal(panicfStr, loggerMock.LastCall())
	s.Equal(panicfStr, loggerMock.Call(8))

	s.Equal(9, len(loggerMock.Calls))

	loggerMock.Clear()

	s.Equal(0, len(loggerMock.Calls))
}
