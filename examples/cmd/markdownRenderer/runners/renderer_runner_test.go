package runners

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/rstudio/platform-lib/examples/cmd/markdownRenderer/queuetypes"
	"github.com/rstudio/platform-lib/pkg/rscache/test"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/pkg/rsstorage"
	"gopkg.in/check.v1"
)

func TestRendererRunner(t *testing.T) { check.TestingT(t) }

type RendererRunnerSuite struct {
	tempdir test.TempDirHelper
}

var _ = check.Suite(&RendererRunnerSuite{})

var sampleMarkdown = `
# Markdown Test Document

# Subheading

Hi, here's some text with a [link](https://www.google.com).'
`

var expectedHtml = `<h1 id="markdown-test-document">Markdown Test Document</h1>

<h1 id="subheading">Subheading</h1>

<p>Hi, here&rsquo;s some text with a <a href="https://www.google.com" target="_blank">link</a>.&rsquo;</p>
`

func (s *RendererRunnerSuite) TestType(c *check.C) {
	c.Assert(RendererWork{}.Type(), check.Equals, queuetypes.WorkTypeMarkdown)
}

func (s *RendererRunnerSuite) TestAddress(c *check.C) {
	r := RendererWork{SHA: "mysha"}
	c.Assert(r.Address(), check.Equals, "v1_markdown_mysha.html")
}

func (s *RendererRunnerSuite) TestNewRendererWork(c *check.C) {
	r, err := NewRendererWork(sampleMarkdown)
	c.Assert(err, check.IsNil)

	c.Assert(r.SHA, check.Equals, "de20414117eeca031a8bb70917117be85bf2d65e56a65557184e71f742e99fc6")
}

func (s *RendererRunnerSuite) TestNewRunner(c *check.C) {
	server := &rsstorage.DummyStorageServer{}
	r := NewRendererRunner(server)
	c.Check(r, check.DeepEquals, &RendererRunner{
		server: server,
	})
}

func (s *RendererRunnerSuite) TestRunUnmarshalError(c *check.C) {
	server := &rsstorage.DummyStorageServer{}
	r := NewRendererRunner(server)
	bWork := []byte("bad")
	err := r.Run(queue.RecursableWork{
		Work: bWork,
	})
	c.Check(err, check.ErrorMatches, "invalid character 'b' looking for beginning of value")
}

func (s *RendererRunnerSuite) TestRunCacheError(c *check.C) {
	server := &rsstorage.DummyStorageServer{
		PutErr: errors.New("cache error"),
	}
	r := NewRendererRunner(server)
	w, err := NewRendererWork(sampleMarkdown)
	c.Assert(err, check.IsNil)
	bWork, err := json.Marshal(w)
	c.Assert(err, check.IsNil)
	err = r.Run(queue.RecursableWork{
		Work: bWork,
	})
	c.Check(err, check.ErrorMatches, `renderer_runner error: cache error`)
}

func (s *RendererRunnerSuite) TestRunOk(c *check.C) {
	server := &rsstorage.DummyStorageServer{}
	r := NewRendererRunner(server)
	w, err := NewRendererWork(sampleMarkdown)
	c.Assert(err, check.IsNil)
	bWork, err := json.Marshal(w)
	c.Assert(err, check.IsNil)
	err = r.Run(queue.RecursableWork{
		Work: bWork,
	})
	c.Check(err, check.IsNil)
	c.Check(server.PutCalled, check.Equals, 1)
}

func (s *RendererRunnerSuite) TestResolverSuccess(c *check.C) {
	server := &rsstorage.DummyStorageServer{}
	r := NewRendererRunner(server)
	w := bytes.NewBuffer([]byte{})
	work, err := NewRendererWork(sampleMarkdown)
	c.Assert(err, check.IsNil)
	_, _, err = r.getResolverText(&work)(w)
	c.Assert(err, check.IsNil)

	// Check the output.
	c.Assert(w.String(), check.Equals, expectedHtml)
}
