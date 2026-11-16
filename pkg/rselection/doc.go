// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package rselection provides leader election for a group of nodes.
//
// One node in the group is the leader. The others are followers. Only the
// leader runs cluster-wide background work. When the leader stops, another
// node takes over.
//
// The package defines three things:
//   - Leader and ContextLeader, the interfaces for a leader loop.
//   - TaskHandler, which runs the leader's tasks for as long as the node is
//     leading.
//   - Task and Schedule, which describe the work. A task is either persistent
//     (starts once and runs until the leader stops) or scheduled (runs each
//     time its schedule fires).
//
// GenericTaskHandler, GenericTask and IntervalSchedule are ready-made
// implementations. The leaders live in the subpackages impls/local, for a
// single node, and impls/pgx, for several nodes that share a PostgreSQL
// database. The single-node leader implements Leader. The PostgreSQL leader
// takes a context in Lead, so it implements ContextLeader. Leaders use the
// broadcaster from the rsnotify package to talk to other nodes.
package rselection
