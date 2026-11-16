# `/pkg/rsstorage/servers/postgres`

## Description

> The Postgres implementation is alpha/experimental. It needs more testing
> (and probably more work) to prepare it for actual use.

A storage server implementation that uses PostgreSQL large object
storage. Requires a connection pool from the
[pgx library](https://github.com/jackc/pgx).

## Constructing a server

```go
pool, err := pgxpool.New(ctx, "postgres://user:password@host/dbname")
if err != nil {
	// ...
}

server := postgres.NewStorageServer(postgres.StorageServerArgs{
	Class:     "docs",
	Pool:      pool,
	ChunkSize: 1 << 20,
	Waiter:    myWaiter,
	Notifier:  myNotifier,
})
```

| Argument | Meaning |
| --- | --- |
| `Pool` | A `*pgxpool.Pool` from `github.com/jackc/pgx/v5/pgxpool`. |
| `Class` | A name for this server. Use a non-empty value. It becomes the first part of every stored address, so several servers can share one table. `Enumerate` returns only the addresses that start with `Class + "/"`. |
| `ChunkSize`, `Waiter`, `Notifier` | For chunked storage. See [servers](../README.md). |

## What the database needs

The server does not create any tables. It expects a `large_objects` table that
maps an address to the identifier of a PostgreSQL large object. The tests use
this definition:

```sql
CREATE TABLE large_objects (
	oid INTEGER PRIMARY KEY,
	address TEXT UNIQUE NOT NULL
);
```

The address is stored as `<class>/<dir>/<address>`. The server reads and writes
the data through the PostgreSQL large object functions, inside transactions.

## Behavior

- `Put` creates a large object, adds a row with a temporary address, and
  renames the row to its final address in the same transaction. An existing
  row with the same address is deleted first.
- `Get` of a regular item holds a database connection and an open transaction
  until you close the reader. Always close it, and make sure the pool has
  enough connections for the readers you keep open.
- `Check` does not return a modification time for a regular item. It returns
  the zero time.
- `Move` to another `postgres` server rewrites the address in this server's own
  `large_objects` table. Use it only when both servers use the same database.
  `Copy` always reads and writes the data.
- `CalculateUsage` returns an error. `Flush` does nothing.
- `Dir()` returns `pg:` followed by the class.
- `EnumeratePrefix` puts the prefix in the SQL query, so only matching rows
  are read. The characters `%`, `_`, and `\` in a prefix match themselves.
