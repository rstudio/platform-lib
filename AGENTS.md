# AGENTS.md

Instructions for coding agents working in this repo.

## What this repo is

`github.com/rstudio/platform-lib/v4` is a set of shared Go libraries. Other applications import it, so treat every exported identifier under `pkg/` as public API. A breaking change to exported API needs a new major version, which also means changing the `/vN` suffix in the module path and in every import path (see Releases).

It is one Go module. There is a single `go.mod` at the repo root. Do not add another one. Some `README.md` files under `pkg/` say each implementation should have its own `go.mod`; that is out of date, because there is only the root module.

## Layout

- `pkg/rscache`: file and memory caches.
- `pkg/rselection`: leader election and task handling. `impls/local` and `impls/pgx` are the implementations, `electiontypes` holds shared types.
- `pkg/rskey`: only a README that points to `github.com/rstudio/rskey`. No Go code.
- `pkg/rslog`: logging library that wraps `github.com/sirupsen/logrus`. `rslogtest` has logger mocks and helpers for tests. Release notes for it are in `pkg/rslog/NEWS.md`.
- `pkg/rsnotify`: notifications. `listener`, `notifier`, `broadcaster` and `listenerfactory` define the pieces. `listeners/{local,postgrespgx,postgrespq}` are Listener implementations.
- `pkg/rsqueue`: work queues. `queue`, `agent`, `groups`, `permit`, `runnerfactory`, `metrics`, `types`, `utils` are the core. `impls/database` is the database implementation.
- `pkg/rsstorage`: storage servers. The interface and shared code are in `pkg/rsstorage` and `types`. `servers/{file,postgres,s3server}` are the implementations.
- `internal/util`: private helpers.
- `examples/cmd`: small example programs. `just build` builds them.
- `docs`, `scripts`, `docker`, `.devcontainer`: docs, helper scripts, integration test compose file, VS Code dev container.

## Commands

Run these from the repo root. Go 1.26 and [just](https://github.com/casey/just) are required.

| Task | Command |
| --- | --- |
| Unit tests, whole repo | `just test` |
| One package tree | `just test ./pkg/rslog/...` |
| One gocheck suite, verbose | `just test -v ./pkg/rsnotify/listeners/local/... -check.f=LocalNotifySuite` |
| Skip the test cache | `just test -count 1 ./...` |
| Integration tests (needs Docker) | `just test-integration` or `just test-integration ./pkg/rsstorage/internal/integration_test/...` |
| Vet | `just vet` |
| Format, header and test-wiring checks | `just lint` |
| Build the examples into `out/` | `just build` |
| Regenerate `NOTICE.md` (needs Python 3) | `just licenses` |

Extra arguments to `just test` go straight to `go test`. `just test` adds `-short` by default. `just test-integration` does not use `-short`, and it starts Postgres and MinIO with `docker/docker-compose.test.yml`.

## What CI checks

The `Platform Lib Pipeline` workflow runs `just lint`, `just vet`, then `just test-integration`. All three must pass.

- `just lint` runs `scripts/fmt-check.sh` (`gofmt -s`), `scripts/header-check.sh` and `scripts/test-wiring.sh`.
- Header: line 3 of every `.go` file must match `// Copyright (C) YYYY by RStudio, PBC` or `// Copyright (C) YYYY by Posit Software, PBC`. Existing files put the `package` line first, then a blank line, then the copyright line, then the imports. A file that starts with a `//go:build` line has it on line 1, a blank line 2, and the copyright on line 3. For new files, use `Posit Software, PBC` with the current year, which is the form `.golangci.yml` expects.
- Test wiring: every package that has tests must call `check.TestingT(t)` (gocheck) or `suite.Run` (testify). Every exported `type XxxSuite struct` in a `_test.go` file must be registered once, with `var _ = check.Suite(&XxxSuite{})` or `suite.Run(t, &XxxSuite{})`. Suites with lowercase names are ignored. A suite registered twice, or not at all, fails the check.
- `.golangci.yml` exists (errcheck, govet, staticcheck, goimports and others, with import groups for `github.com/rstudio`), but CI does not run `golangci-lint`. Keep imports grouped the same way anyway.
- `.editorconfig`: UTF-8, LF line endings, final newline, no trailing whitespace, tabs in Go files, two-space indent in YAML and JSON.

## Test conventions

- Most tests use `gopkg.in/check.v1` (gocheck). Name the entry point `TestPackage` or similar, for example `func TestPackage(t *testing.T) { check.TestingT(t) }`, and put test methods on a suite struct. A few packages use `testify/suite`. Follow the style of the package you are editing.
- Tests that need Postgres, MinIO or other services must skip when `testing.Short()` is true, because `just test` runs with `-short`. Example: `c.Skip("... because -short was provided")` in `SetUpSuite`.
- `pkg/rsstorage/internal/integration_test` and `pkg/rscache/internal/integration_test` run shared tests across several implementations. They live under `internal` so that importers of `pkg/` do not pull in every implementation and its dependencies.
- Logger mocks for tests are in `pkg/rslog/rslogtest`.

## Releases

The repo is versioned by tag, for the whole module at once, with semantic versioning. To release, push a tag like `v4.3.0` on the merge commit on `main`. The `Release` workflow then creates the GitHub Release with generated notes. You can also run the workflow manually with a `version` input, and it creates the tag from `main`.

- Do not create a GitHub Release by hand. Only push the tag.
- Do not tag or release unless asked.
- A major bump also requires changing the `/vN` suffix in `go.mod` and in all import paths.

## Do not

- Do not add a second `go.mod`.
- Do not change exported API in `pkg/` without saying it is breaking.
- Do not edit `NOTICE.md` by hand. Regenerate it with `just licenses`.
