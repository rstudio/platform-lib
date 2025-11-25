package runners

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
	"github.com/pkg/errors"
	"github.com/rstudio/platform-lib/v3/examples/cmd/markdownRenderer/queuetypes"
	"github.com/rstudio/platform-lib/v3/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/v3/pkg/rsstorage"
	storagetypes "github.com/rstudio/platform-lib/v3/pkg/rsstorage/types"
)

// RendererRunner is a runner is registered with the queue agent that knows how
// to render Markdown text to HTML.
type RendererRunner struct {
	queue.BaseRunner

	// Knows how to retrieve or create cached files
	server rsstorage.StorageServer
}

func NewRendererRunner(server rsstorage.StorageServer) *RendererRunner {
	return &RendererRunner{
		server: server,
	}
}

// RendererWork is the work that gets serialized and "Pushed" into the queue
// when we need to render markdown to HTML.
type RendererWork struct {
	Markdown string
	SHA      string
}

func NewRendererWork(markdown string) (RendererWork, error) {
	hash := sha256.New()
	_, err := hash.Write([]byte(markdown))
	if err != nil {
		return RendererWork{}, err
	}

	// Encode the SHA of markdown.
	sha := hex.EncodeToString(hash.Sum(nil))
	return RendererWork{
		Markdown: markdown,
		SHA:      sha,
	}, nil
}

func (RendererWork) Type() uint64 {
	return queuetypes.WorkTypeMarkdown
}

// Address is used to determine the file name for storing the rendered HTML. Since the
// address includes the SHA of the provided markdown, we can guarantee a unique address.
// Additionally, if rendering is requested for the same markdown again, the address will
// match an existing item in the cache.
func (w RendererWork) Address() string {
	return fmt.Sprintf("v1_markdown_%s.html", w.SHA)
}

// Dir specifies a subdirectory in which to store an item on the storage server. If blank,
// no subdirectory will be used.
func (w RendererWork) Dir() string {
	return ""
}

func (r *RendererRunner) Run(ctx context.Context, work queue.RecursableWork) error {

	job := &RendererWork{}
	err := json.Unmarshal(work.Work, &job)
	if err != nil {
		return err
	}

	for {
		// Render the markdown to HTML and "Put" it into storage.
		_, _, err = r.server.Put(ctx, r.getResolverText(job), job.Dir(), job.Address())
		if err != nil {
			return errors.Wrap(err, "renderer_runner error")
		} else {
			return nil
		}
	}
}

func (r *RendererRunner) getResolverText(job *RendererWork) storagetypes.Resolver {

	return func(writer io.Writer) (string, string, error) {
		// Load support for some popular markdown extensions and create a parser.
		var extensions = parser.NoIntraEmphasis |
			parser.Tables |
			parser.FencedCode |
			parser.Autolink |
			parser.Strikethrough |
			parser.SpaceHeadings |
			parser.AutoHeadingIDs
		p := parser.NewWithExtensions(extensions)

		// Specify some helpful flags and create a renderer.
		htmlFlags := html.CommonFlags | html.HrefTargetBlank
		opts := html.RendererOptions{Flags: htmlFlags}
		renderer := html.NewRenderer(opts)

		// Render the markdown and write it to storage.
		normalized := markdown.NormalizeNewlines([]byte(job.Markdown))
		renderedHtml := markdown.ToHTML(normalized, p, renderer)
		_, err := writer.Write(renderedHtml)
		return "", "", err
	}
}
