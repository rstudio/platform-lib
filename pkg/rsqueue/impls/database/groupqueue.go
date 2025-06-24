package database

// Copyright (C) 2022 by Posit, PBC

import (
	"encoding/json"

	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/groups"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
)

type GroupQueueFactory interface {
	// NewGroup returns a new GroupQueue.
	NewGroup(job groups.GroupQueueJob) (groups.GroupQueue, error)

	// GetGroup wraps an existing QueueGroup in the GroupQueue interface
	GetGroup(group groups.GroupQueueJob) groups.GroupQueue
}

type DefaultQueueGroupFactory struct {
	baseQueue  queue.Queue
	groupQueue queue.Queue
}

type QueueGroupFactoryConfig struct {
	BaseQueue  queue.Queue
	GroupQueue queue.Queue
}

func NewQueueGroupFactory(cfg QueueGroupFactoryConfig) *DefaultQueueGroupFactory {
	return &DefaultQueueGroupFactory{
		baseQueue:  cfg.BaseQueue,
		groupQueue: cfg.GroupQueue,
	}
}

func (qf *DefaultQueueGroupFactory) GetGroup(g groups.GroupQueueJob) groups.GroupQueue {
	return &DefaultGroupQueue{
		BaseQueue:  qf.baseQueue,
		GroupQueue: qf.groupQueue,
		group:      g,
	}
}

func (qf *DefaultQueueGroupFactory) NewGroup(job groups.GroupQueueJob) (q groups.GroupQueue, err error) {
	return &DefaultGroupQueue{
		BaseQueue:  qf.baseQueue,
		GroupQueue: qf.groupQueue,
		group:      job,
	}, nil
}

type DefaultGroupQueue struct {
	BaseQueue  queue.Queue
	GroupQueue queue.Queue
	group      groups.GroupQueueJob
}

func (q *DefaultGroupQueue) Push(priority uint64, work queue.Work) error {
	return q.BaseQueue.Push(priority, q.group.GroupId(), work)
}

func (q *DefaultGroupQueue) Start() error {
	return q.GroupQueue.Push(0, 0, q.group)
}

func (q *DefaultGroupQueue) SetEndWork(work interface{}, endWorkType uint8) error {
	b, err := json.Marshal(work)
	if err != nil {
		return err
	}

	q.group.SetEndWork(endWorkType, b)
	return nil
}

func (q *DefaultGroupQueue) Group() groups.GroupQueueJob {
	return q.group
}

func (q *DefaultGroupQueue) BaseQueueName() string {
	return q.BaseQueue.Name()
}
