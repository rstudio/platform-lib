package test

// Copyright (C) 2025 by Posit Software, PBC

import (
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/check.v1"
)

// TempDirHelper helps tests create and destroy temporary directories.
type TempDirHelper struct {
	prefix string
	dir    string
}

// NewTempDirHelper creates a helper that can create and destroy temporary
// directories.
func NewTempDirHelper() TempDirHelper {
	return TempDirHelper{
		prefix: "rstudio-queue-test-",
	}
}

// SetUp creates a temporary directory
func (h *TempDirHelper) SetUp() error {
	var err error
	h.dir, err = ioutil.TempDir("", h.prefix)
	return err
}

// TearDown removes the configured directory
func (h *TempDirHelper) TearDown() error {
	var err error
	if h.dir != "" {
		err = os.RemoveAll(h.dir)
		h.dir = ""
	}
	return err
}

// Dir returns the path to the configured directory
func (h *TempDirHelper) Dir() string {
	return h.dir
}

type timeEquals struct {
	*check.CheckerInfo
}

// TimeEquals is a checker that uses time.Time.Equal to compare time.Time objects.
var TimeEquals check.Checker = &timeEquals{
	&check.CheckerInfo{Name: "TimeEquals", Params: []string{"obtained", "expected"}},
}

func (checker *timeEquals) Check(params []interface{}, names []string) (result bool, error string) {
	if obtained, ok := params[0].(time.Time); !ok {
		return false, "obtained is not a time.Time"
	} else if expected, ok := params[1].(time.Time); !ok {
		return false, "expected is not a time.Time"
	} else {
		// We cannot do a DeepEquals on Time because Time is not
		// comparable through reflection.
		return obtained.Unix() == expected.Unix(), ""
	}
}
