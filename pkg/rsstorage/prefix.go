package rsstorage

// Copyright (C) 2026 by Posit Software, PBC

import (
	"context"
	"errors"
	"strings"

	"github.com/rstudio/platform-lib/v4/pkg/rsstorage/types"
)

// ErrPrefixEnumerationUnsupported is returned by EnumeratePrefix when the
// underlying storage server cannot scope a listing by prefix. Callers should
// treat it as "ask for less, or do without" rather than falling back to a full
// Enumerate: the whole point of the prefix API is to avoid listing everything,
// so a silent fallback reintroduces the cost the caller was avoiding.
var ErrPrefixEnumerationUnsupported = errors.New("storage server does not support prefix enumeration")

// PrefixEnumerator is implemented by storage servers that can list a subset of
// their contents identified by a key prefix, without enumerating everything.
//
// It is deliberately a separate interface rather than a method on StorageServer.
// StorageServer is a published surface with implementers outside this module, so
// adding a method to it would be a breaking change requiring a major version
// bump (see the release policy in README.md). Callers type-assert instead:
//
//	if pe, ok := server.(PrefixEnumerator); ok {
//		items, err := pe.EnumeratePrefix(ctx, "SOME_PREFIX")
//	}
//
// Every storage server in this module implements it. A decorator that wraps a
// StorageServer must forward it explicitly; embedding the StorageServer
// interface does not promote methods that interface does not declare, so the
// type assertion above would otherwise fail on the wrapper and quietly lose the
// optimization.
type PrefixEnumerator interface {
	// EnumeratePrefix returns every stored item whose key begins with prefix.
	//
	// A key is the item's Dir and Address joined with "/", or just Address for
	// items at the root of the server. Prefixes are matched over that whole
	// string and need not fall on a path boundary: the prefix "GIT_" matches the
	// root-level key "GIT_1_2_3.gob", and the prefix "a/b" matches both "a/bc"
	// and "a/b/c".
	//
	// An empty prefix is equivalent to Enumerate.
	//
	// Dir is relative to the root of the storage server, so a returned item can
	// be passed straight back to Get, Check or Remove.
	EnumeratePrefix(ctx context.Context, prefix string) ([]types.StoredItem, error)
}

// ItemKey returns the key that PrefixEnumerator matches against: the item's
// directory and address joined with "/", or just the address at the root.
func ItemKey(dir, address string) string {
	if dir == "" {
		return address
	}
	return dir + "/" + address
}

// KeyHasPrefix reports whether the item identified by dir and address is within
// prefix.
func KeyHasPrefix(dir, address, prefix string) bool {
	return strings.HasPrefix(ItemKey(dir, address), prefix)
}

// SubtreeMayMatch reports whether a directory whose path relative to the storage
// root is dir could contain any key matching prefix. It is the pruning test for
// a tree walk: a false result means the whole subtree can be skipped.
//
// Two cases qualify. Either the prefix reaches down into this directory, so a
// match may lie deeper ("a" when the prefix is "a/b/c"), or the directory is
// itself already within the prefix, so everything below it matches ("GIT_x" when
// the prefix is "GIT_"). The root, whose relative path is empty, always
// qualifies.
func SubtreeMayMatch(dir, prefix string) bool {
	if dir == "" || prefix == "" {
		return true
	}
	// The prefix descends into this directory. Compare against dir + "/" so that
	// a sibling sharing a name prefix -- dir "ab" against prefix "a/b" -- does
	// not look like a match.
	if strings.HasPrefix(prefix, dir+"/") {
		return true
	}
	// The prefix stops short of this directory's name, so the directory and
	// everything under it is inside the prefix.
	return strings.HasPrefix(dir, prefix)
}
