package servertest

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"time"

	"gopkg.in/check.v1"

	"github.com/rstudio/platform-lib/v3/pkg/rsstorage/types"
)

type DummyChunkUtils struct {
	WriteErr error
	Read     io.ReadCloser
	ReadCh   *types.ChunksInfo
	ReadSz   int64
	ReadMod  time.Time
	ReadErr  error
}

func (f *DummyChunkUtils) WriteChunked(ctx context.Context, dir, address string, sz uint64, resolve types.Resolver) error {
	return f.WriteErr
}

func (f *DummyChunkUtils) ReadChunked(ctx context.Context, dir, address string) (io.ReadCloser, *types.ChunksInfo, int64, time.Time, error) {
	return f.Read, f.ReadCh, f.ReadSz, f.ReadMod, f.ReadErr
}

type DummyWaiterNotifier struct {
	Ch chan bool
}

func (d *DummyWaiterNotifier) WaitForChunk(ctx context.Context, c *types.ChunkNotification) {
	to := time.NewTimer(time.Second)
	defer to.Stop()
	select {
	case <-d.Ch:
	case <-to.C:
	}
}

func (d *DummyWaiterNotifier) Notify(ctx context.Context, c *types.ChunkNotification) error {
	select {
	case d.Ch <- true:
	default:
	}
	return nil
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

// TempDirHelper helps tests create and destroy temporary directories.
type TempDirHelper struct {
	prefix string
	dir    string
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

// TestDESC is a test description that is used for various tests.
const TestDESC = `Encoding: UTF-8
Package: plumber
Type: Package
Title: An API Generator for R
Version: 0.4.2
Date: 2017-07-24
Authors@R: c(
  person(family="Trestle Technology, LLC", role="aut", email="cran@trestletech.com"),
  person("Jeff", "Allen", role="cre", email="cran@trestletech.com"),
  person("Frans", "van Dunné", role="ctb", email="frans@ixpantia.com"),
  person(family="SmartBear Software", role=c("ctb", "cph"), comment="swagger-ui"))
License: MIT + file LICENSE
BugReports: https://github.com/trestletech/plumber/issues
URL: https://www.rplumber.io (site)
        https://github.com/trestletech/plumber (dev)
Description: Gives the ability to automatically generate and serve an HTTP API
    from R functions using the annotations in the R documentation around your
    functions.
Depends: R (>= 3.0.0)
Imports: R6 (>= 2.0.0), stringi (>= 0.3.0), jsonlite (>= 0.9.16),
        httpuv (>= 1.2.3), crayon
LazyData: TRUE
Suggests: testthat (>= 0.11.0), XML, rmarkdown, PKI, base64enc,
        htmlwidgets, visNetwork, analogsea
LinkingTo: testthat (>= 0.11.0), XML, rmarkdown
Enhances: testthat (>= 0.12.0), XML, rmarkdown
Collate: 'content-types.R' 'cookie-parser.R' 'parse-globals.R'
        'images.R' 'parse-block.R' 'globals.R' 'serializer-json.R'
        'shared-secret-filter.R' 'post-body.R' 'query-string.R'
        'plumber.R' 'default-handlers.R' 'digital-ocean.R'
        'find-port.R' 'includes.R' 'paths.R' 'plumber-static.R'
        'plumber-step.R' 'response.R' 'serializer-content-type.R'
        'serializer-html.R' 'serializer-htmlwidget.R'
        'serializer-xml.R' 'serializer.R' 'session-cookie.R'
        'swagger.R'
RoxygenNote: 6.0.1
NeedsCompilation: no
Packaged: 2017-07-24 17:17:15 UTC; jeff
Author: Trestle Technology, LLC [aut],
  Jeff Allen [cre],
  Frans van Dunné [ctb],
  SmartBear Software [ctb, cph] (swagger-ui)
Maintainer: Jeff Allen <cran@trestletech.com>
Repository: CRAN
Date/Publication: 2017-07-24 21:50:56 UTC
`
