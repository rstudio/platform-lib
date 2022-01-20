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

## Go Modules

Each subdirectory of `/pkg` is a separate Go module and should contain its own
`go.mod`. You can release an individual module easily by creating a tag that 
contains the subdirectory. For example, to release `rslog` `1.0.1`, you would
create a tag like this:

```bash
git tag pkg/rslog/v1.0.1
git push origin --tags
```

Before creating a tag, ensure that the `go.mod` has been updated with the
correct dependency versions for other `platform-lib` packages. For example,
the local listener module at `/pkg/rsnotify/listeners/local` depends upon the
`/pkg/rsnotify` module, so we must ensure that the local listener's `go.mod`
references the correct `rsnotify` version.
