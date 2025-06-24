package types

// Copyright (C) 2022 by Posit, PBC

import "math"

const (
	// TYPE_NONE represents work without a type that gets inserted into the job
	// queue. TYPE_NONE is set to a high number to avoid conflicts. We don't know
	// which data store this value will be coupled to, so we use the max int32
	// value for safety. SQLite, for example, only supports signed integers,
	// so `math.MaxUint64` is too large.
	//
	// However, we support uint64 for the work type for more flexibility with
	// data stores that support large numbers.
	TYPE_NONE uint64 = math.MaxInt32
)
