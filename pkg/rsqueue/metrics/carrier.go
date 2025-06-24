package metrics

// Copyright (C) 2025 By Posit Software, PBC

import (
	"context"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

// CarrierFactory provides a method for retrieving a byte array of tracing data. This
// data should be saved/persisted with the job by the queue implementation when work
// is pushed to the queue; it should also be loaded by the queue implementation when work
// is retrieved from the queue.
type CarrierFactory interface {
	GetCarrier(label, queueName, address string, priority, workType uint64, group int64) []byte
}

type JobLifecycleWrapper interface {
	Start(ctx context.Context, work *queue.QueueWork) (context.Context, interface{}, error)
	Enqueue(queueName string, work queue.Work, err error) error
	Dequeue(queueName string, work queue.Work, err error) error
	Finish(data interface{})
}

type EmptyCarrierFactory struct{}

func (*EmptyCarrierFactory) GetCarrier(label, queueName, address string, priority, workType uint64, group int64) []byte {
	return nil
}

type EmptyJobLifecycleWrapper struct{}

func (*EmptyJobLifecycleWrapper) Start(ctx context.Context, work *queue.QueueWork) (context.Context, interface{}, error) {
	return ctx, nil, nil
}
func (*EmptyJobLifecycleWrapper) Enqueue(queueName string, work queue.Work, err error) error {
	return nil
}
func (*EmptyJobLifecycleWrapper) Dequeue(queueName string, work queue.Work, err error) error {
	return nil
}
func (*EmptyJobLifecycleWrapper) Finish(data interface{}) {}
