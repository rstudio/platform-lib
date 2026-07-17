# RStudio Platform Go Libraries

## Overview

This repo includes useful Go code for building applications.

## Go Directories

### `/internal`

Private application and library code. This is for code we don't want others
importing in their applications or libraries. Note that this layout pattern is
enforced by the Go compiler itself. See the Go 1.4
[`release notes`](https://golang.org/doc/go1.4#internalpackages) for more
details. Note that you are not limited to the top level `internal` directory.
You can have more than one `internal` directory at any level of your project
tree.

You can optionally add a bit of extra structure to your internal packages to
separate your shared and non-shared internal code. It's not required (especially
for smaller projects), but it's nice to have visual clues showing the intended
package use. Your actual application code can go in the `/internal/app`
directory (e.g., `/internal/app/myapp`) and the code shared by those apps in the
`/internal/pkg` directory (e.g., `/internal/pkg/myprivlib`).

### `/pkg`

Shared library code for use by external applications (e.g., `/pkg/rslog`).
Other projects can import these libraries expecting them to work.

### `/examples`

Example applications that demonstrate using the shared libraries.

## Common Application Directories

### `/scripts`

Scripts to perform various build, install, linting, and documentation tasks.

These scripts keep the root level `justfile` small and simple (e.g.,
[`https://github.com/hashicorp/terraform/blob/master/Makefile`](https://github.com/hashicorp/terraform/blob/master/Makefile)).

## Other Directories

### `/docs`

Design and user documents (in addition to your godoc generated documentation).

## Tooling

We use `just` to run project specific commands. See the 
[GitHub repository](https://github.com/casey/just) for installation and 
examples.

You will also need [Docker](https://docs.docker.com/get-docker/) if you wish to 
run the integration and end-to-end tests.

## Testing

`just test` runs the Go unit tests for the whole module. Any arguments are
passed through to `go test`, so a package pattern scopes the run to specific
packages.

Examples:

```bash
# Run all Go tests
just test

# Run all tests once (no cached results)
just test -count 1 ./...

# Run with verbose output
just test -v ./...

# Test a single package subtree
just test ./pkg/rslog/...

# Run one gocheck suite in a package, verbose
just test -v ./pkg/rsnotify/listeners/local/... -check.f=LocalNotifySuite
```

### Testing with Docker

The integration tests are run with Docker. 

Examples:

```bash
# Run the integration tests (uses docker-compose)
just test-integration
```

## Licenses

To update `NOTICE.md` with a list of licenses from third-party Go modules,
use the `just licenses` target. This requires Python 3.

## Release

The repo is a single Go module (`github.com/rstudio/platform-lib/v4`), so a
single tag versions the whole library. Follow semantic versioning.

To release, tag the merge commit on `main` and push it. The `Release` workflow
(`.github/workflows/release.yml`) then publishes a GitHub Release with an
auto-generated changelog.

```shell
git tag v4.3.0
git push origin v4.3.0
```

Or run the workflow manually to create the tag and release in one step,
without touching local git:

```shell
gh workflow run release.yml -f version=v4.3.1
```

A major bump (e.g. v3 to v4) also requires updating the `/vN` suffix in the
module path and all import paths, not just the tag.

## Badges

* [Go Report Card](https://goreportcard.com/) - It will scan your code with
  `gofmt`, `go vet`, `gocyclo`, `golint`, `ineffassign`, `license` and
  `misspell`. Replace `github.com/golang-standards/project-layout` with your
  project reference.

    [![Go Report Card](https://goreportcard.com/badge/github.com/rstudio/platform-lib?style=flat-square)](https://goreportcard.com/report/github.com/rstudio/platform-lib)

* [Pkg.go.dev](https://pkg.go.dev) - Pkg.go.dev is a new destination for Go
  discovery & docs. You can create a badge using the
  [badge generation tool](https://pkg.go.dev/badge).

    [![PkgGoDev](https://pkg.go.dev/badge/github.com/rstudio/platform-lib)](https://pkg.go.dev/github.com/rstudio/platform-lib)

* Release - It will show the latest release number for your project. Change the
  github link to point to your project.

    [![Release](https://img.shields.io/github/release/rstudio/platform-lib.svg?style=flat-square)](https://github.com/rstudio/platform-lib/releases/latest)
