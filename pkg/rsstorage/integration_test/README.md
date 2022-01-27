# `/pkg/rsstorage/integration_test`

## Description

Includes unit tests that exercise all the storage servers
implemented in `pkg/rsstorage/servers/*`. Since these tests must import
all the implementation modules, we package them separately so other 
applications don't need to import all the implementations and their
dependencies.

These tests can be run with:

```bash
# All tests
just test-integration

# All tests in this module
MODULE=pkg/rsstorage/integration_test just test-integration
```
