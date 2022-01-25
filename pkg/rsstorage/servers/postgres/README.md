# `/pkg/rsstorage/servers/postgres`

## Description

> The Postgres implementation is alpha/experimental. It needs more testing
> (and probably more work) to prepare it for actual use.

A persistent storage server implementation that uses PostgreSQL large object
storage. Requires a connection pool from the
[pgx library](https://github.com/jackc/pgx). 

