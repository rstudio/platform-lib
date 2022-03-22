package groups

// Copyright (C) 2022 by RStudio, PBC

import (
	"fmt"
)

type GroupQueueEndRunnerFactory interface {
	AddRunner(t uint8, runner GroupQueueEndRunner)
	GetRunner(t uint8) (GroupQueueEndRunner, error)
}

type GroupQueueEndRunner interface {
	Run(work []byte) error
}

func NewGroupQueueEndRunnerFactory() *DefaultGroupQueueEndRunnerFactory {
	return &DefaultGroupQueueEndRunnerFactory{
		runners: make(map[uint8]GroupQueueEndRunner, 0),
	}
}

type DefaultGroupQueueEndRunnerFactory struct {
	runners map[uint8]GroupQueueEndRunner
}

func (r *DefaultGroupQueueEndRunnerFactory) AddRunner(t uint8, runner GroupQueueEndRunner) {
	r.runners[t] = runner
}

func (r *DefaultGroupQueueEndRunnerFactory) GetRunner(t uint8) (GroupQueueEndRunner, error) {
	runner, ok := r.runners[t]
	if !ok {
		return nil, fmt.Errorf("Could not find GroupEndRunner for type %d", t)
	}

	return runner, nil
}
