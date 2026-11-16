// This file holds the package documentation.

// Copyright (C) 2026 by Posit Software, PBC

// Package localelection provides a leader for a single node.
//
// LocalLeader assumes that the node it runs on is always the leader. It
// starts the task handler, answers every verification request from scheduled
// tasks with true, and stops the task handler when its stop channel fires.
// Use it when there is only one node and no election is needed.
package localelection
