package queue

// Copyright (C) 2025 By Posit Software, PBC

import (
	"errors"
	"sync"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/permit"
)

type addParams struct {
	Item    Work
	Address string
}

var kaboom = errors.New("RecordingProducer function not implemented")

// RecordingProducer is an implementation of queue.Queue which records
// arguments passed to its methods. RecordingProducer is meant to be used in
// tests.
type RecordingProducer struct {
	AddParams     []addParams
	PushError     error
	Extended      int
	PollErrs      chan error
	RecordErr     error
	HasAddress    bool
	HasAddressErr error
	// Notified when the first Push or AddressedPush is received
	Received chan bool
	Lock     sync.Mutex
	Peeked   []uint64
	PeekRes  []QueueWork
	PeekErr  error
}

func (q *RecordingProducer) WithDbTx(tx interface{}) Queue {
	return q
}

func (q *RecordingProducer) Peek(filter func(work *QueueWork) (bool, error), types ...uint64) ([]QueueWork, error) {
	if q.PeekErr != nil {
		return nil, q.PeekErr
	}
	if q.Peeked == nil {
		q.Peeked = types
	} else {
		q.Peeked = append(q.Peeked, types...)
	}
	results := make([]QueueWork, 0)
	for _, w := range q.PeekRes {
		ok, err := filter(&w)
		if err != nil {
			return nil, err
		}
		if ok {
			results = append(results, w)
		}
	}
	return results, nil
}

func (q *RecordingProducer) Push(priority uint64, groupId int64, work Work) error {
	q.AddParams = append(q.AddParams, addParams{work, ""})
	q.Lock.Lock()
	defer q.Lock.Unlock()
	if q.Received != nil {
		q.Received <- true
		q.Received = nil
	}
	return q.PushError
}

func (q *RecordingProducer) AddressedPush(priority uint64, groupId int64, address string, work Work) error {
	q.AddParams = append(q.AddParams, addParams{work, address})
	q.Lock.Lock()
	defer q.Lock.Unlock()
	if q.Received != nil {
		q.Received <- true
		q.Received = nil
	}
	return q.PushError
}

func (q *RecordingProducer) IsAddressInQueue(address string) (bool, error) {
	return q.HasAddress, q.HasAddressErr
}

func (q *RecordingProducer) PollAddress(address string) (errs <-chan error) {
	return q.PollErrs
}

func (q *RecordingProducer) RecordFailure(address string, failure error) error {
	return q.RecordErr
}

func (q *RecordingProducer) Get(maxPriority uint64, maxPriorityChan chan uint64, types QueueSupportedTypes, stop chan bool) (*QueueWork, error) {
	return nil, kaboom
}

func (q *RecordingProducer) Extend(permit.Permit) error {
	q.Extended += 1
	return nil
}

func (q *RecordingProducer) Delete(permit.Permit) error {
	return kaboom
}

func (q *RecordingProducer) Name() string {
	return ""
}
