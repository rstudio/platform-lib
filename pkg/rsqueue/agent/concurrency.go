package agent

// Copyright (C) 2022 by RStudio, PBC

import (
	"errors"
	"math"
	"sort"
	"sync"
)

const MAX_CONCURRENCY = uint64(math.MaxInt32)

type ConcurrencyEnforcer struct {
	concurrencies map[int64]int64
	mutex         sync.RWMutex
}

// Int64RevSlice makes []int64 sortable with sort.Sort (reverse order)
type Int64RevSlice []int64

func (a Int64RevSlice) Len() int           { return len(a) }
func (a Int64RevSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Int64RevSlice) Less(i, j int) bool { return a[i] > a[j] }
func (p Int64RevSlice) Sort()              { sort.Sort(p) }

// Returns a slice of priorities that are configured. This is the list
// or priorities needed for the queue.
func uniquePriorities(priorities []int64) []int64 {
	result := make([]int64, 0)
	for _, i := range priorities {
		if !Int64InSlice(i, result) {
			result = append(result, i)
		}
	}
	return result
}

func Int64InSlice(needle int64, haystack []int64) bool {
	for _, h := range haystack {
		if needle == h {
			return true
		}
	}
	return false
}

// Returns a map of concurrency limits, where the key
// is the priority and the value is the concurrency limit
// for that priority.
func Concurrencies(defaults, priorityMap map[int64]int64, priorities []int64) (*ConcurrencyEnforcer, error) {
	cEnforcer := &ConcurrencyEnforcer{}

	// Set the initial concurrency values based on config
	err := cEnforcer.SetConcurrencies(defaults, priorityMap, priorities)
	if err != nil {
		return nil, err
	}

	return cEnforcer, nil
}

func (c *ConcurrencyEnforcer) SetConcurrencies(defaults, priorityMap map[int64]int64, priorities []int64) error {
	// Get a list of priorities configured
	ps := uniquePriorities(priorities)

	// build the initial map with defaults
	concurrencies := make(map[int64]int64)
	for _, priority := range ps {
		if max, ok := defaults[priority]; ok {
			concurrencies[priority] = max
		}
	}

	for priority, max := range priorityMap {
		concurrencies[priority] = max
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.concurrencies = concurrencies
	return nil
}

// Returns keys sorted in reverse order
func (c *ConcurrencyEnforcer) sort() []int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var keys []int64
	for k := range c.concurrencies {
		keys = append(keys, k)
	}
	sort.Sort(Int64RevSlice(keys))

	return keys
}

// Returns an error if any priority has a concurrency setting
// lower than the a low priority.
func (c *ConcurrencyEnforcer) Verify() error {
	keys := c.sort()

	var currentHigh int64

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Walk in reverse order (lower priority first)
	//
	// We assume that lower priorities (higher key) should have concurrency settings
	// equal to or lower than higher priorities
	for _, key := range keys {
		val := c.concurrencies[key]

		// If the concurrency setting has increased, record the current high
		if currentHigh < val {
			currentHigh = val
		}

		// If the concurrency setting has decreased, it's an error
		if val < currentHigh {
			return errors.New("Higher priorities may not have lower concurrency " +
				"settings than lower priorities")
		}
	}

	return nil
}

// Check if we have the capacity to run any jobs
// Parameters:
//  * jobCount - the number of running jobs
// Returns:
//  * bool  - True if we have capacity to take a job
//  * int64 - The maximum priority job we have capacity to run
func (c *ConcurrencyEnforcer) Check(jobCount int64) (bool, uint64) {
	keys := c.sort()

	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Walk in reverse order (lower priority first)
	for _, key := range keys {
		val := c.concurrencies[key]

		// If our concurrency settings allow more capacity at this level
		if jobCount < val {
			maxPriority := uint64(key)

			// Special case: if we have capacity for the minimum priority,
			// then return a large value so we also accommodate any remaining
			// jobs with priorities higher than we've currently configured
			if keys[0] == key {
				maxPriority = MAX_CONCURRENCY
			}

			return true, maxPriority
		}
	}

	return false, 0
}
