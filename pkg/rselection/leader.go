package rselection

// Copyright (C) 2022 by RStudio, PBC

type DebugLogger interface {
	Debugf(message string, args ...interface{})
	Enabled() bool
}

type Leader interface {
	Lead() error
}
