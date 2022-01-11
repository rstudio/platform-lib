# RStudio Platform Go Libraries

## Overview

This repo includes useful Go code for building applications.

## Go Directories

### `/internal`

Private application and library code. This is the code you don't want others importing in their applications or libraries. Note that this layout pattern is enforced by the Go compiler itself. See the Go 1.4 [`release notes`](https://golang.org/doc/go1.4#internalpackages) for more details. Note that you are not limited to the top level `internal` directory. You can have more than one `internal` directory at any level of your project tree.

You can optionally add a bit of extra structure to your internal packages to separate your shared and non-shared internal code. It's not required (especially for smaller projects), but it's nice to have visual clues showing the intended package use. Your actual application code can go in the `/internal/app` directory (e.g., `/internal/app/myapp`) and the code shared by those apps in the `/internal/pkg` directory (e.g., `/internal/pkg/myprivlib`).

### `/pkg`

Library code that's ok to use by external applications (e.g., `/pkg/mypubliclib`). Other projects will import these libraries expecting them to work, so think twice before you put something here.

## Common Application Directories

### `/scripts`

Scripts to perform various build, install, analysis, etc operations.

These scripts keep the root level `justfile` small and simple (e.g., [`https://github.com/hashicorp/terraform/blob/master/Makefile`](https://github.com/hashicorp/terraform/blob/master/Makefile)).

### `/test`

Additional external test apps and test data.

## Other Directories

### `/docs`

Design and user documents (in addition to your godoc generated documentation).

### `/examples`

Examples for your applications and/or public libraries.

## Testing

Examples:

```bash
# Run all Go tests
just test

# Run all Go tests twice
just test "-count 2"

# Run all tests once (no cached results)
just test "-count 1"

# Run with verbose output
just test "-v"

# Run the "TestNewDebugLog" test twice with verbose output
just test "-count 2 -testify.m TestNewDebugLog -v"

# Run the end-to-end tests
just build
just test-e2e

# Open an interactive container for end-to-end testing
just start-e2e-env
just test
exit
```

## Licenses

To update `NOTICE.md` with a list of licenses from third-party Go modules,
use the `just licenses` target. This requires Python 3.

## Versioning

Follow [semantic versioning](https://go.dev/doc/modules/version-numbers) guidelines. To release a new version, we simply
create and push a tag.

```shell
git tag v0.1.2
git push origin v0.1.2
```

## Release Checklist

- [ ] Ensure build passes on `main` branch.
- [ ] Update `README.md` as necessary.
- [ ] Update `NOTICE.md`. See [Licenses](#licenses).
- [ ] Tag the new version. See [Versioning](#versioning)

## Badges

* [Go Report Card](https://goreportcard.com/) - It will scan your code with `gofmt`, `go vet`, `gocyclo`, `golint`, `ineffassign`, `license` and `misspell`. Replace `github.com/golang-standards/project-layout` with your project reference.

    [![Go Report Card](https://goreportcard.com/badge/github.com/golang-standards/project-layout?style=flat-square)](https://goreportcard.com/report/github.com/golang-standards/project-layout)

* ~~[GoDoc](http://godoc.org) - It will provide online version of your GoDoc generated documentation. Change the link to point to your project.~~

    [![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/golang-standards/project-layout)

* [Pkg.go.dev](https://pkg.go.dev) - Pkg.go.dev is a new destination for Go discovery & docs. You can create a badge using the [badge generation tool](https://pkg.go.dev/badge).

    [![PkgGoDev](https://pkg.go.dev/badge/github.com/golang-standards/project-layout)](https://pkg.go.dev/github.com/golang-standards/project-layout)

* Release - It will show the latest release number for your project. Change the github link to point to your project.

    [![Release](https://img.shields.io/github/release/golang-standards/project-layout.svg?style=flat-square)](https://github.com/golang-standards/project-layout/releases/latest)
