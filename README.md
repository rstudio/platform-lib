# RStudio Platform Go Libraries

[![PkgGoDev](https://pkg.go.dev/badge/github.com/rstudio/platform-lib/v4)](https://pkg.go.dev/github.com/rstudio/platform-lib/v4)
[![Go Report Card](https://goreportcard.com/badge/github.com/rstudio/platform-lib/v4?style=flat-square)](https://goreportcard.com/report/github.com/rstudio/platform-lib/v4)
[![Release](https://img.shields.io/github/release/rstudio/platform-lib.svg?style=flat-square)](https://github.com/rstudio/platform-lib/releases/latest)

## Overview

Shared Go libraries for building server applications: logging, storage,
caching, job queues, notifications, and leader election.

The repo is a single Go module. Import a package with its full path:

```go
import "github.com/rstudio/platform-lib/v4/pkg/rslog"
```

## Packages

| Package | Purpose |
| --- | --- |
| [`pkg/rscache`](pkg/rscache/README.md) | Caches files and objects on disk and in memory. Works with `rsstorage` and `rsqueue`. |
| [`pkg/rselection`](pkg/rselection/README.md) | Leader election for a cluster of nodes, with tasks that run on the leader. Works with `rsnotify`. |
| [`pkg/rslog`](pkg/rslog/README.md) | Logging. Wraps `github.com/sirupsen/logrus`. |
| [`pkg/rsnotify`](pkg/rsnotify/README.md) | Sends and listens for notifications, in one process or across nodes through PostgreSQL. |
| [`pkg/rsqueue`](pkg/rsqueue/README.md) | A database-backed job queue. |
| [`pkg/rsstorage`](pkg/rsstorage/README.md) | File storage with one interface over a file system, PostgreSQL, or S3. |

`pkg/rskey` holds no code. It points to
[rstudio/rskey](https://github.com/rstudio/rskey), which lives in its own repo.

API reference for every package is on
[pkg.go.dev](https://pkg.go.dev/github.com/rstudio/platform-lib/v4).

## Examples

The example applications live in `/examples/cmd`. Build them all with
`just build`. The binaries go to `out/`.

| Example | Shows |
| --- | --- |
| [`markdownRenderer`](examples/cmd/markdownRenderer/README.md) | `rscache`, `rsnotify`, `rsqueue`, and `rsstorage` working together to render Markdown to HTML. |
| `testlog` | Logging with `rslog`. |
| `testnotify` | Sending and receiving notifications with `rsnotify`. |
| `testservice` | Changing the `log/slog` level and format when the process receives `SIGHUP`. |

## Repo Layout

| Directory | Contents |
| --- | --- |
| `/pkg` | The shared libraries. Other projects import these. |
| `/examples` | Example applications that use the libraries. |
| `/internal` | Code that other modules cannot import. The Go compiler enforces this. |
| `/scripts` | Shell scripts for testing, linting, and license reports. The `justfile` calls them. |
| `/docker` | The Docker Compose file for the integration tests. |

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

Do not create the release manually via the GitHub UI — just create the tag and
let the workflow handle the release.

A major bump (e.g. v3 to v4) also requires updating the `/vN` suffix in the
module path and all import paths, not just the tag.
