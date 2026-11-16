# `pkg/rselection`

## Description

A lightweight leader election library. It is intended for use with
[`rsnotify`](../rsnotify/README.md).

In a group of nodes, one node is the leader and the others are followers. The
leader runs background work that must run on only one node at a time. This
library provides the pieces for that work and two ways to choose a leader:

- An implementation for PostgreSQL (using pgx) for several nodes.
- An implementation for a single node. It assumes that the node is always the
  leader.

## Examples

- [`impls/local/example_test.go`](impls/local/example_test.go) runs a leader
  with one persistent task and one scheduled task.
- [`example_test.go`](example_test.go) shows how a task handler asks the leader
  for permission before each scheduled run.
- [`impls/pgx/example_test.go`](impls/pgx/example_test.go) shows how to wire a
  follower and a leader for PostgreSQL. It is compiled but not run, because it
  needs a database.

## Definitions

- **Leader** The node that currently runs the cluster's background work. A
  leader loop runs a `TaskHandler` for as long as the node is leading. The
  single-node leader implements the `Leader` interface (`Lead()`). The
  PostgreSQL leader implements `ContextLeader` (`Lead(ctx)`). It passes the
  context to the notifications it sends. Cancelling the context does not end
  the leader loop.

- **Follower** A node that is not the leader. In the PostgreSQL implementation,
  a follower answers the leader's pings. If it hears no pings for too long, it
  asks for a new leader.

- **Task** One unit of leader work. A task has a name, a type, a schedule, and
  a `Run` method. `Run` receives a context and a broadcaster from `rsnotify`.

  - A **persistent** task (`TaskTypePersistent`) starts once, when the task
    handler starts. It runs until the task handler stops, which cancels the
    context passed to `Run`.
  - A **scheduled** task (`TaskTypeScheduled`) runs each time its schedule
    fires. Each run gets its own goroutine.

- **Schedule** Tells a scheduled task when to run. `Next` returns a channel of
  times, and the task runs each time a value arrives. `IntervalSchedule` wraps
  a channel that you provide, for example the channel of a `time.Ticker`.

- **Task handler** Implements the `TaskHandler` interface. It starts the
  registered tasks (`Handle`) and stops them (`Stop`). `GenericTaskHandler` is
  the provided implementation. `GenericTask` is a helper that holds the name,
  type and schedule, so your own task only needs to add `Run`.

- **Verify** Before each scheduled run, the task handler sends a request on the
  channel returned by `Verify`. The leader reads the request and answers `true`
  or `false`. On `false`, that run is skipped. The single-node leader always
  answers `true`. The PostgreSQL leader answers `true` only when the cluster
  looks healthy.

The `electiontypes` package holds the messages that nodes send each other, such
as pings, ping responses, and node lists. It also holds `AssumeLeader`, the
item that a follower queues when it wants a new leader.

## How it relates to `rsnotify`

The broadcaster from `rsnotify` is how a leader hears about messages from other
nodes. The leader subscribes to the messages it needs, such as ping responses.
It also passes the broadcaster to every task, so a task can subscribe to
notifications too. The PostgreSQL implementation sends its own messages with
PostgreSQL `NOTIFY`, and receives them through a `postgrespgx` listener that
feeds the broadcaster.

## Implementations

### `impls/local`

Package name: `localelection`. For a single node. `LocalLeader` starts the task
handler when `Lead` is called, answers every verification request with `true`,
and stops the task handler when its stop channel fires. `Lead` blocks until
then. It does not need a database.

### `impls/pgx`

Package name: `pgxelection`. For several nodes that share a PostgreSQL
database.

- `PgxLeader` pings the followers on a timer and tracks which nodes answer. It
  also sweeps nodes that stopped answering. It runs the task handler while it
  leads. It answers verification requests with two checks: its last ping attempt
  succeeded, and every node in the database (from the `ClusterPgStore` you
  provide) is in its own list of known nodes. That list starts from the
  database when the leader starts, and the sweep removes nodes whose last ping
  is too old. If it hears a ping from another leader, it stops leading. If
  `StepDownTimeout` is set and the cluster stays unhealthy for that long, it
  also stops, so that a new election can happen.
- `PgxFollower` answers the leader's pings. If it hears no ping for `Timeout`,
  it pushes an `electiontypes.AssumeLeader` item to the `Queue` you provide.
  The library does not process that queue. Calling `Promote` makes `Follow`
  return `FollowResultPromote`.
- `PgxPgNotifier` sends messages with PostgreSQL `NOTIFY`. Channel names longer
  than 63 characters are replaced by an MD5 hash, because PostgreSQL limits
  the length.

Use `impls/local` when there is one node. Use `impls/pgx` when several nodes
share a PostgreSQL database.

## Usage

This walk-through uses the single-node leader. The full, runnable version is in
[`impls/local/example_test.go`](impls/local/example_test.go).

1. Write your tasks. Embed `rselection.GenericTask` and add a `Run` method.

   ```go
   type sweepTask struct {
       rselection.GenericTask
   }

   func (t *sweepTask) Run(ctx context.Context, b broadcaster.Broadcaster) {
       // Do the work.
   }
   ```

2. Create the task handler and register the tasks. Register them before the
   leader starts. Registering after `Handle`, or registering the same name
   twice, ends the program with `log.Fatalf`.

   ```go
   sweep := &sweepTask{
       GenericTask: rselection.GenericTask{
           TaskName:     "sweep",
           TaskType:     rselection.TaskTypeScheduled,
           TaskSchedule: &rselection.IntervalSchedule{Ticker: time.NewTicker(time.Minute).C},
       },
   }
   handler := rselection.NewGenericTaskHandler(rselection.GenericTaskHandlerConfig{})
   handler.Register(sweep.Name(), sweep)
   ```

3. Create the leader and call `Lead`. It blocks until the stop channel fires.

   ```go
   stop := make(chan bool)
   leader := localelection.NewLocalLeader(localelection.LocalLeaderConfig{
       Broadcaster: awb, // from rsnotify
       TaskHandler: handler,
       StopChannel: stop,
   })
   err := leader.Lead()
   ```

For several nodes, each node runs a `PgxFollower`. A node that is promoted
creates a `PgxLeader` and calls `Lead(ctx)`. When `Lead` returns, the node goes
back to following. See [`impls/pgx/example_test.go`](impls/pgx/example_test.go).

`GenericTaskHandlerConfig` has an optional `OnTaskRun` callback, which is
called before each scheduled run. `PgxLeaderConfig` has optional
`OnIntegrityResult` and `OnPingResult` callbacks. `OnPingResult` may run concurrently with itself and with
`OnIntegrityResult`, so both callbacks must be safe for concurrent use and must
not block.

## Testing

Run the unit tests with:

```
just test ./pkg/rselection/...
```

This passes `-short`. The `GenericTaskHandler` tests, the `electiontypes`
tests, the `impls/local` tests and the Postgres-free leader recovery tests in
`impls/pgx` run. The other `impls/pgx` test suites (leader, follower and
notifier) skip themselves under `-short`, because they need PostgreSQL.

Run everything, including those suites, with Docker:

```
just test-integration ./pkg/rselection/...
```

This starts a PostgreSQL container and runs the tests without `-short`.
