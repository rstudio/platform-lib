package metrics

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"

	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
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
