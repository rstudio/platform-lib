package handlers

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/runners"
	"github.com/rstudio/platform-lib/v2/pkg/rscache"
)

type HttpHandler struct {
	router  *mux.Router
	address string
	cache   rscache.FileCache
}

func NewHttpHandler(address string, router *mux.Router, cache rscache.FileCache) *HttpHandler {
	return &HttpHandler{
		router:  router,
		address: address,
		cache:   cache,
	}
}

// Start starts the HTTP service and listens for requests.
func (h *HttpHandler) Start(ctx context.Context) {
	// Set up routes that we can handle
	h.router.HandleFunc("/", h.Home).Methods("GET")
	h.router.HandleFunc("/render", h.Render).Methods("POST")

	srv := &http.Server{
		Handler:      h.router,
		Addr:         h.address,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	// Listen for HTTP requests
	go srv.ListenAndServe()

	// Wait until the context is cancelled, which signals that it's time to shut down.
	<-ctx.Done()

	// Create a context with a 30-second timeout, and attempt to shut down HTTP services.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	slog.Debug("Shutting down HTTP services.")
	srv.Shutdown(shutdownCtx)
}

// Home serves the root `GET /` route.
func (h *HttpHandler) Home(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(homeHtml))
}

// Render serves the `POST /render` route that renders a markdown document.
func (h *HttpHandler) Render(w http.ResponseWriter, r *http.Request) {
	// Render the markdown to HTML.
	renderedHtml, err := h.render(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("error: %s", err)))
		return
	}

	// Write to the browser.
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(fmt.Sprintf(renderHtml, renderedHtml)))
}

// render uses the cache to render a markdown document.
func (h *HttpHandler) render(r *http.Request) (rendered string, err error) {
	// Get the markdown from the form data that was POSTed.
	markdown := r.PostFormValue("markdown")

	// Create a markdown rendering job for the queue.
	work, err := runners.NewRendererWork(markdown)
	if err != nil {
		return
	}

	// Add the job to a cache resolver specification.
	spec := rscache.ResolverSpec{
		Priority: 0,
		Work:     work,
	}

	// Get the result from the cache by passing the resolver spec to
	// the cache.
	result := h.cache.Get(context.Background(), spec)
	if result.Err != nil {
		err = result.Err
		return
	}

	// The resulting object is a file, so retrieve it as an `io.Reader`.
	// When instead retrieving an object from the cache, use
	// `result.AsObject()`.
	renderedReader, err := result.AsReader()
	if err != nil {
		return
	}

	renderedBytes, err := ioutil.ReadAll(renderedReader)
	if err != nil {
		return
	}

	rendered = string(renderedBytes)
	return
}

// Template for home page (`/`).
var homeHtml = `
<html>
  <head>
	<title>Render Markdown</title>
	<style>
		textarea {
			width: 100%;
			height: 50%;
		}
	</style>
  </head>
  <body>
    <p>Enter some markdown below:</p>
    <form method="POST" action="render">
      <textarea name="markdown" id="markdown"></textarea>
      <input type="submit" value="Render Markdown" />
    </form>
  </body>
</html>
`

// Template for rendered markdown page (`/render`)
var renderHtml = `
<html>
  <head><title>Render Markdown</title></head>
  <body>
    %s
  </body>
</html>
`
