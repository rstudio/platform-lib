# `pkg/rsqueue`

## Description

This module provides a database-backed queue implementation.

## Examples

- [markdownRenderer](../../examples/cmd/markdownRenderer/README.md)
  demonstrates how to use `rsqueue` to schedule and complete work.

## Definitions

The important pieces of a working queue include:

- **Queue** Implements the `queue.Queue` interface. A collection of work for a specific `name`.

- **Work** Data that is stored in the queue. Must be serializable to JSON. Each row in the `queue` table
  represents one unit of work.

* **Permit** A permit is an integer that gives a worker the right to handle a single unit of work in the
  queue.

* **Agent** Retrieves available work from the queue and handles it.

* **Runner** Runs work.

* **Scheduler** Inserts work into a queue.

## Details

The `queue` and `queue_permit` tables are displayed below with example data.

| id | name | priority | group_id | address | permit | item |
| --- | ---   | --- | --- | ---  | ---   | ---    |
| 1   | jobs  |0    |   0 | add1 |     1 | []byte |
| 2   | jobs  |1    |   2 | NULL |  NULL | []byte |
| 3   | jobs  |0    |   2 | NULL |     2 | []byte |
| 4   | sweep |0    |   0 | NULL |  NULL | []byte |
| 5   | sweep |2    |   0 | NULL |  NULL | []byte |

Table:  The data backing a queue

| id | heartbeat |
| -- | -- |
| 1  | 1504699964 |
| 2  | 1504692874 |

Table:  The data backing queue permits

A queue is a collection of work records. All work records with the same unique name are considered a queue. If
you need multiple queues, simply use different names. The `priority` column allows you to prioritize work.
Lower numbers have higher priority, so zero (0) is the highest priority.

To claim work, we first insert a record in the `queue_permit` table. Next, we identify unclaimed work (`WHERE
permit=NULL`) in the `queue` table and mark it with the permit id.

The `heartbeat` value in the `queue_permit` table must be periodically updated. If the heartbeat expires, the
permit is deleted, and any work in the queue marked with the permit is updated with `permit=NULL` to make the
work available again.

We currently implement a database-backed queue that supports both SQLite3 and Postgres. See the `queue/database`
package for this implementation. We also include POC queue implementation backed by RabbitMQ. See
`cmd/queue-test/rabbitmq` for this implementation.

### Work

Each record in the `queue` table is a single unit of "work". Work must be serializable to JSON. A unit of work
can be associated (`name`) with a specific named queue, and it can be assigned a priority (zero is the highest
priority).

Work can also be addressed. In addition to the `Push` method, the queue interface includes `AddressedPush`.
If you attempt to push work with a duplicate address into the queue, you'll receive the error
`queue.ErrDuplicateAddressedPut`. Typically, it's ok to ignore `queue.ErrDuplicateAddressedPut` since it
means that the queue already contains the work you're attempting to insert.

The queue interface includes `PollAddress` for polling addressed work for completion. `PollAddress` provides
an error channel, and when the addressed work item is removed from the queue (due to completion, deletion,
or failure), a result (error or nil) is returned over the error channel.

### Permit

See _Table_ for more information. A permit has a `heartbeat` value that must be updated periodically. If a
permit's heartbeat becomes stale, the permit will be canceled, and any work associated with the permit will
become available for another worker to claim.

### Agent

The agent should be relatively generic, and should be able to retrieve and handle work without being coupled
to any types associated with the work. Agents may enforce concurrency limits, and hence may be tightly coupled
to concurrency requirements for a specific type of work. Currently, we don't include any interfaces associated
with Agents; Agents are designed independently.

### Runner

Since the agent should not be coupled tightly to the type of work it handles, we recommend creating a Runner
that knows how to run a specific type of work. The Runner is also responsible for extending queue permits.

### Scheduler

A scheduler inserts work into a queue. Our queue does not define any scheduler interfaces. When running with
multiple nodes in a cluster, it is important to avoid scheduling the same work multiple times. One way to
prevent this is by using master elections.

## Queue Groups

### Description of Queue Groups

We include a database-backed (PostgreSQL/SQLite) grouped queue implementation. See `queue/database/groupqueue.go`
for more information. A GroupQueue provides a few important methods:

1. Push - Push work into the queue group. You can also push work into a particular group using the `groupId`
parameter of the normal `queue.Push` or `queue.AddressedPush` methods.
2. SetEndWork - Specifies work that is handled when the group is complete.
3. Start - Starts monitoring the queue group for completion. When complete, the queue group's "end work" is
performed.

### Starting a Queue Group

The queue agent will not retrieve work marked for a specific group from the queue until that group is started.
When the queue group is started, any agent can retrieve work marked for that group, and processing for group
work commences using the same concurrency and priority settings as any other non-group work. Make sure that
you never `Start` a queue group until you can guarantee that an empty group means that the group is completed.
Behind the scenes, the `Start` method simply pushes a work item flagged with QUEUE_GROUP_START into the
queue.

The `queue/groups/queuegrouprunner` monitors the queue group for completion. When the record count of queued
work for the group id is zero (0), the group is considered complete, and the "end" work is inserted into the
queue (not the GroupQueue).

### Canceling a Queue Group

If you push work flagged with QUEUE_GROUP_CANCEL into the queue, the associated queue group will be cancelled.
Any remaining work for this queue group is immediately deleted from the queue, and the queue group is marked
as canceled. The queue group continues monitoring for a zero-work-count for the queue group. When the group
is empty, we then push a QUEUE_GROUP_ABORT work item into the queue to handle any cleanup associated with
cancelling the group.

## Queue Proof-of-Concept

See the `cmd/queue-test/poc` package for a queue POC that inserts work into a queue, retrieves work from the
queue, and runs the work.
