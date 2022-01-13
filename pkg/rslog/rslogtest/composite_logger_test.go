package rslogtest

// Copyright (C) 2022 by RStudio, PBC.

import (
	"io"
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/stretchr/testify/mock"
)

func (s *LoggerImplTestSuite) TestNewCompositeLogger() {

	mock1 := &LoggerMock{}
	mock2 := &LoggerMock{}
	mocks := []*LoggerMock{mock1, mock2}

	logger := rslog.NewCompositeLogger([]rslog.Logger{mock1, mock2})

	// Test common signature log functions
	testCases := []struct {
		Name        string
		TestAction  func(l rslog.Logger)
		LogFunction string
	}{
		{
			Name:        "Test Tracef",
			TestAction:  func(l rslog.Logger) { l.Tracef("Message") },
			LogFunction: "Tracef",
		},
		{
			Name:        "Test Debugf",
			TestAction:  func(l rslog.Logger) { l.Debugf("Message") },
			LogFunction: "Debugf",
		},
		{
			Name:        "Test Infof",
			TestAction:  func(l rslog.Logger) { l.Infof("Message") },
			LogFunction: "Infof",
		},
		{
			Name:        "Test Warnf",
			TestAction:  func(l rslog.Logger) { l.Warnf("Message") },
			LogFunction: "Warnf",
		},
		{
			Name:        "Test Errorf",
			TestAction:  func(l rslog.Logger) { l.Errorf("Message") },
			LogFunction: "Errorf",
		},
		{
			Name:        "Test Fatalf",
			TestAction:  func(l rslog.Logger) { l.Fatalf("Message") },
			LogFunction: "Fatalf",
		},
		{
			Name:        "Test Panicf",
			TestAction:  func(l rslog.Logger) { l.Panicf("Message") },
			LogFunction: "Panicf",
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.Name, func(t *testing.T) {
			for _, m := range mocks {
				m.On(tc.LogFunction, "Message", mock.Anything)
			}

			tc.TestAction(logger)

			for _, m := range mocks {
				m.AssertExpectations(s.T())
			}
		})
	}

	//Test Fatal function
	for _, m := range mocks {
		m.On("Fatal", []interface{}{"Message"})
	}

	logger.Fatal("Message")

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}

	//Test WithField function

	newMock1 := &LoggerMock{}
	mock1.On("WithField", "field", "value").Return(newMock1)

	newMock2 := &LoggerMock{}
	mock2.On("WithField", "field", "value").Return(newMock2)

	newComposite := logger.WithField("field", "value")

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}

	s.EqualValues(newComposite, rslog.NewCompositeLogger([]rslog.Logger{newMock1, newMock2}))

	//Test WithFields function

	mock1.On("WithFields", rslog.Fields{"field": "value"}).Return(newMock1)
	mock2.On("WithFields", rslog.Fields{"field": "value"}).Return(newMock2)

	newComposite = logger.WithFields(rslog.Fields{"field": "value"})

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}

	s.EqualValues(newComposite, rslog.NewCompositeLogger([]rslog.Logger{newMock1, newMock2}))

	// Test SetLevel function

	for _, m := range mocks {
		m.On("SetLevel", rslog.InfoLevel)
	}

	logger.SetLevel(rslog.InfoLevel)

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}

	// Test SetOutput function
	for _, m := range mocks {
		m.On("SetOutput", []io.Writer{IoWriterMock{}})
	}

	logger.SetOutput(IoWriterMock{})

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}

	// Test SetFormatter function
	for _, m := range mocks {
		m.On("SetFormatter", rslog.TextFormat)
	}

	logger.SetFormatter(rslog.TextFormat)

	for _, m := range mocks {
		m.AssertExpectations(s.T())
	}
}
