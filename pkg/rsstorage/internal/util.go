package internal

// Copyright (C) 2022 by RStudio, PBC

import (
	"math/rand"
	"strings"
)

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}

	return b
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// RandomString generates a random string of length n. The characters
// drawn are alphanumeric. The first string is always a letter.
// The strings are not cryptographically random.
func RandomString(n int) string {
	b := make([]byte, n)
	b[0] = alpha[rand.Intn(len(alpha))]
	for i := 1; i < n; i++ {
		b[i] = alnum[rand.Intn(len(alnum))]
	}
	return string(b)
}

// Similar to to `strings.Join`, but ignores blank strings
func NotEmptyJoin(a []string, sep string) string {
	b := make([]string, 0)
	for _, item := range a {
		if item != "" {
			b = append(b, item)
		}
	}
	return strings.Join(b, sep)
}
