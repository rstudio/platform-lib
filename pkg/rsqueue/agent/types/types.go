package agenttypes

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/listener"
)

type Agent interface {
	Run(ctx context.Context, notify Notify)
	Stop(timeout time.Duration) error
	Wait(ctx context.Context, runningJobs int64, jobDone chan int64) uint64
}

type Notify func(n listener.Notification)

// WorkCompleteNotification A notification that indicates addressed queue work is complete.
type WorkCompleteNotification struct {
	listener.GenericNotification
	Address string
}

func NewWorkCompleteNotification(address string, workType uint8) *WorkCompleteNotification {
	return &WorkCompleteNotification{
		GenericNotification: listener.GenericNotification{
			NotifyGuid: uuid.New().String(),
			NotifyType: workType,
		},
		Address: address,
	}
}
