# `/pkg/rscache/internal/integration_test`

## Description

Includes unit tests that exercise the cache against a file storage server
implemented in `pkg/rsstorage/servers/file`. Since these tests import the
file server implementation, they live in this `internal` package so other
applications don't pull in the implementations and their dependencies.

These tests can be run with:

```bash
# All tests
just test

# Just these tests
just test ./pkg/rscache/internal/integration_test/...
```
