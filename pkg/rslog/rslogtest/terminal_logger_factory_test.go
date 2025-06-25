package rslogtest

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"os"
	"testing"

	"github.com/rstudio/platform-lib/pkg/rslog"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
)

type TerminalLoggerFactorySuite struct {
	suite.Suite
}

func TestTerminalLoggerFactorySuite(t *testing.T) {
	suite.Run(t, &TerminalLoggerFactorySuite{})
}

func (s *TerminalLoggerFactorySuite) TestTerminalLoggerFactory() {
	factory := &rslog.TerminalLoggerFactory{
		LogLevel: rslog.InfoLevel,
	}

	lgr := factory.DefaultLogger()

	s.NotNil(lgr)
	s.IsType(&rslog.LoggerImpl{}, lgr)

	lgr_impl := lgr.(*rslog.LoggerImpl)

	s.Equal(logrus.InfoLevel, lgr_impl.CoreLogger.Level)
	s.Equal(os.Stderr, lgr_impl.CoreLogger.Out)
	s.IsType(&rslog.UTCTextFormatter{}, lgr_impl.CoreLogger.Formatter)
}
