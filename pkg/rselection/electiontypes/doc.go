// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package electiontypes defines the messages and data types that nodes
// exchange during leader election.
//
// The messages are pings and ping responses between the leader and followers,
// and a request and response that list the nodes in the cluster. Each message
// has a numeric type, such as ClusterMessageTypePing, that subscribers use to
// select the messages they want. ClusterNode describes one node, and
// AssumeLeader is the work item that a follower queues when it asks for a new
// leader.
package electiontypes
