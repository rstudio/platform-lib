package localelection

// Copyright (C) 2022 by Posit Software, PBC

import (
	"github.com/rstudio/platform-lib/v2/pkg/rselection"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
)

type LocalLeader struct {
	awb         broadcaster.Broadcaster
	taskHandler rselection.TaskHandler

	// Used to shut down this service
	stop chan bool
}

type LocalLeaderConfig struct {
	Broadcaster broadcaster.Broadcaster
	TaskHandler rselection.TaskHandler
	StopChannel chan bool
}

func NewLocalLeader(cfg LocalLeaderConfig) *LocalLeader {
	return &LocalLeader{
		awb:         cfg.Broadcaster,
		taskHandler: cfg.TaskHandler,
		stop:        cfg.StopChannel,
	}
}

func (p *LocalLeader) Lead() error {
	// Start handling leader tasks
	p.taskHandler.Handle(p.awb)
	defer p.taskHandler.Stop()

	p.lead(p.stop)
	return nil
}

func (p *LocalLeader) lead(stop chan bool) {
	for {
		select {
		case <-stop:
			return
		case vCh := <-p.taskHandler.Verify():
			if vCh != nil {
				vCh <- true
			}
		}
	}
}
