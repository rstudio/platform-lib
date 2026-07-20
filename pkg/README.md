# `/pkg`

## Description

Shared library code for use by external applications (e.g., `/pkg/rslog`). Other
projects will import these libraries expecting them to work, so think twice
before you put something here. Note that the `internal` directory is a better
way to ensure your private packages are not importable because it's enforced by
Go. The `/pkg` directory is still a good way to explicitly communicate that code
is safe for use by others. The
[`I'll take pkg over internal`](https://travisjeffery.com/b/2019/11/i-ll-take-pkg-over-internal/)
blog post by Travis Jeffery provides a good overview of the `pkg` and `internal`
directories and when it might make sense to use them.

It's also a way to group Go code in one place when your root directory contains
lots of non-Go components and directories making it easier to run various Go
tools.

## Go Module

Everything under `/pkg` is part of the single root module
(`github.com/rstudio/platform-lib/v4`) and is versioned by the repo's top-level
tags. See the "Release" section of the top-level [README](../README.md) for how
releases are cut.
