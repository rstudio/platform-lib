# `/pkg/rsstorage/internal/integration_test`

## Description

Includes unit tests that exercise all the storage servers
implemented in `pkg/rsstorage/servers/*`. Since these tests import all the
storage server implementations, they live in this `internal` package so other
applications don't pull in those implementations and their dependencies.

These tests can be run with:

```bash
# All tests
just test-integration

# Just these tests
just test-integration ./pkg/rsstorage/internal/integration_test/...
```
