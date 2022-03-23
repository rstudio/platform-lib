# `/pkg/rscache/internal/integration_test`

## Description

Includes unit tests that exercise the cache against a file storage server
implemented in `pkg/rsstorage/servers/file`. Since these tests must import
the file server module, we package them separately so other 
applications don't need to import all the implementations and their
dependencies.

These tests can be run with:

```bash
# All tests
just test

# All tests in this module
MODULE=pkg/rscache/internal/integration_test just test
```
