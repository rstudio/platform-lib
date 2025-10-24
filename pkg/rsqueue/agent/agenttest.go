package agent

// Copyright (C) 2022 by RStudio, PBC

import (
	"context"
	"time"

	agenttypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent/types"
)

type FakeAgent struct {
	StopErr    error
	WaitResult uint64
}

func (a *FakeAgent) Run(ctx context.Context, notify agenttypes.Notify) {
}

func (a *FakeAgent) Stop(timeout time.Duration) error {
	return a.StopErr
}

func (a *FakeAgent) Wait(ctx context.Context, runningJobs int64, jobDone chan int64) uint64 {
	return a.WaitResult
}
