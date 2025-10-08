package agent

// Copyright (C) 2022 by RStudio, PBC

import (
	"time"

	agenttypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent/types"
)

type FakeAgent struct {
	StopErr    error
	WaitResult uint64
}

func (a *FakeAgent) Run(notify agenttypes.Notify) {
}

func (a *FakeAgent) Stop(timeout time.Duration) error {
	return a.StopErr
}

func (a *FakeAgent) Wait(runningJobs int64, jobDone chan int64) uint64 {
	return a.WaitResult
}
