// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package pgxelection provides leader election for several nodes that share
// a PostgreSQL database.
//
// Nodes talk to each other with PostgreSQL NOTIFY messages. PgxLeader pings
// the followers, keeps track of which nodes answer, and runs the task handler
// while it leads. PgxFollower listens for the leader's pings and answers
// them. When a follower hears no ping within its timeout, it pushes an
// electiontypes.AssumeLeader item to a queue that the caller provides. This
// package does not process that queue. Calling Promote on a follower makes its
// Follow method return FollowResultPromote. PgxPgNotifier sends the NOTIFY
// messages.
package pgxelection
