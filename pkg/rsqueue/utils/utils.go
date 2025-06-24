package utils

// Copyright (C) 2025 By Posit Software, PBC

import (
	"fmt"
	"regexp"

	"gopkg.in/check.v1"
)

var lockError = regexp.MustCompile("database .*is locked[^\n]*")

// IsSqliteLockError checks to see if an error is a SQLite3 database locked error
func IsSqliteLockError(err error) bool {
	if err == nil {
		return false
	}
	return lockError.MatchString(err.Error())
}

type sliceEquivalentChecker struct {
	*check.CheckerInfo
}

// SliceEquivalent is a checker that confirms that the haystack contains the needle.
var SliceEquivalent check.Checker = &sliceEquivalentChecker{
	&check.CheckerInfo{Name: "SliceEquivalent", Params: []string{"obtained", "slice"}},
}

// Check ensures that two slices are equivalent
func (checker *sliceEquivalentChecker) Check(params []interface{}, names []string) (result bool, error string) {

	checkInt := func(a, b []int) (bool, string) {
		for i := range a {
			found := false
			for x := range b {
				if a[i] == b[x] {
					found = true
				}
			}
			if !found {
				return false, "slices do not match"
			}
		}
		return true, ""
	}

	checkInt64 := func(a, b []int64) (bool, string) {
		for i := range a {
			found := false
			for x := range b {
				if a[i] == b[x] {
					found = true
				}
			}
			if !found {
				return false, "slices do not match"
			}
		}
		return true, ""
	}

	checkString := func(a, b []string) (bool, string) {
		for i := range a {
			found := false
			for x := range b {
				if a[i] == b[x] {
					found = true
				}
			}
			if !found {
				return false, "slices do not match"
			}
		}
		return true, ""
	}

	checkUint64 := func(a, b []uint64) (bool, string) {
		for i := range a {
			found := false
			for x := range b {
				if a[i] == b[x] {
					found = true
				}
			}
			if !found {
				return false, "slices do not match"
			}
		}
		return true, ""
	}

	switch a := params[0].(type) {
	case []uint64:
		if b, ok := params[1].([]uint64); !ok {
			return false, "slices are not the same type"
		} else if len(a) != len(b) {
			return false, fmt.Sprintf("slices are not the same length: %d != %d", len(a), len(b))
		} else {
			return checkUint64(a, b)
		}
	case []string:
		if b, ok := params[1].([]string); !ok {
			return false, "slices are not the same type"
		} else if len(a) != len(b) {
			return false, fmt.Sprintf("slices are not the same length: %d != %d", len(a), len(b))
		} else {
			return checkString(a, b)
		}
	case []int64:
		if b, ok := params[1].([]int64); !ok {
			return false, "slices are not the same type"
		} else if len(a) != len(b) {
			return false, fmt.Sprintf("slices are not the same length: %d != %d", len(a), len(b))
		} else {
			return checkInt64(a, b)
		}
	case []int:
		if b, ok := params[1].([]int); !ok {
			return false, "slices are not the same type"
		} else if len(a) != len(b) {
			return false, fmt.Sprintf("slices are not the same length: %d != %d", len(a), len(b))
		} else {
			return checkInt(a, b)
		}
	default:
		return false, "unsupported type"
	}
}
