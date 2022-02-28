package rslogtest

// Copyright (C) 2022 by RStudio, PBC.

import (
	"os"
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type LoggerFactoryImplSuite struct {
	suite.Suite
}

func TestLoggerFactoryImplSuite(t *testing.T) {
	suite.Run(t, &LoggerFactoryImplSuite{})
}

func (s *LoggerFactoryImplSuite) TestTerminalLogger() {
	factory := rslog.LoggerFactoryImpl{}

	lgr := factory.TerminalLogger(rslog.InfoLevel)

	s.NotNil(lgr)
	s.IsType(&rslog.LoggerImpl{}, lgr)

	lgr_impl := lgr.(*rslog.LoggerImpl)

	s.Equal(logrus.InfoLevel, lgr_impl.Level)
	s.Equal(os.Stderr, lgr_impl.Out)
	s.IsType(&logrus.TextFormatter{}, lgr_impl.Formatter)
}
