package rselection

// Copyright (C) 2022 by RStudio, PBC

import "context"

// Leader is a leader loop that runs until the node stops leading.
type Leader interface {
	Lead() error
}

// ContextLeader is a leader loop that takes a context. The context is passed
// to the work the leader does while it leads.
type ContextLeader interface {
	Lead(ctx context.Context) error
}
