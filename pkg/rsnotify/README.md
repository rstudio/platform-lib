# `/pkg/rsnotify`

## Description

A notification library that supports listening for notifications. A broadcaster
supports many-to-one listeners per notification, as well as waiting for a
specific notification that matches a filter.

## Examples

- [markdownRenderer](../../examples/cmd/markdownRenderer/README.md)
  demonstrates how to use `rsnotify` to provide notification support
  for storage ([rsstorage](../rsstorage/README.md)), caching
  ([rscache](../rscache/README.md)), and job queueing
  ([rsqueue](../rsqueue/README.md)).
- [testnotify](../../examples/cmd/testnotify) demonstrates sending
  notifications locally (no database) or between processes (using Postgres).  

## Implementations

Each listener implementation is a separate Go module.

### listeners/local

For use in the context of a single process, the local implementation uses
Go channels for notifications. The local implementation, in addition, provides
a notification mechanism for sending the notifications.

### listeners/postgrespgx

A `github.com/jackc/pgx` implementation that supports notifications using
PostgreSQL's `LISTEN` feature. Notifications are sent by using your own code to
`NOTIFY` PostgreSQL.

### listeners/postgrespq

> The `lib/pq` implementation should be considered BETA quality.

A `github.com/lib/pq` implementation that supports notifications using
PostgreSQL's `LISTEN` feature. Notifications are sent by using your own code to
`NOTIFY` PostgreSQL.
