# `/pkg`

Shared library code for use by other applications. Other projects import these
packages and expect them to keep working, so treat every exported name as
public API.

See the package list in the top-level [README](../README.md#packages). Each
package has its own README.

## Go Module

Everything under `/pkg` is part of the single root module
(`github.com/rstudio/platform-lib/v4`) and is versioned by the repo's top-level
tags. See the "Release" section of the top-level [README](../README.md) for how
releases are cut.
