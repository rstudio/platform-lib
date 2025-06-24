package rselection

// Copyright (C) 2025 By Posit Software, PBC

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
)

const (
	TaskTypePersistent uint8 = 0
	TaskTypeScheduled  uint8 = 1

	ScheduleTypeInterval uint8 = 0
)

// A TaskHandler runs in the context of a Leader Loop. When the leader loop ends,
// the TaskHandler stops. A TaskHandler can do two types of work:
//   - Persistent jobs that run for the duration of the TaskHandler's run loop.
//     Persistent jobs, for instance, can monitor a channel and track heartbeats
//     for macro transactions or in-progress queue work.
//   - Scheduled jobs that run periodically. Scheduled jobs have a set schedule
//     and run per that schedule during the lifetime of the TaskHandler's run loop.
type TaskHandler interface {
	Handle(b broadcaster.Broadcaster)
	Register(name string, task Task)
	Stop()
	Verify() <-chan chan bool
}

type Task interface {
	Name() string
	Type() uint8
	Schedule() Schedule
	Run(ctx context.Context, b broadcaster.Broadcaster)
}

type Schedule interface {
	Type() uint8
	Next() <-chan time.Time
}

type GenericTaskHandler struct {
	tasks  map[string]Task
	cancel context.CancelFunc
	verify chan chan bool
	mutex  sync.RWMutex
	role   string
}

type GenericTaskHandlerConfig struct {
}

func NewGenericTaskHandler(cfg GenericTaskHandlerConfig) *GenericTaskHandler {
	return &GenericTaskHandler{
		tasks: make(map[string]Task),
		role:  "Leader",
	}
}

func (h *GenericTaskHandler) Register(name string, task Task) {
	if _, ok := h.tasks[name]; ok {
		log.Fatalf("Attempted to register a task %s that is already registered", name)
	}
	h.tasks[name] = task
}

func (h *GenericTaskHandler) Handle(b broadcaster.Broadcaster) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.verify = make(chan chan bool)
	context, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		for _, t := range h.tasks {
			switch t.Type() {
			case TaskTypePersistent:
				slog.Debug(fmt.Sprintf("%s starting persistent task %s", h.role, t.Name()))
				go t.Run(context, b)
			case TaskTypeScheduled:
				go h.runScheduled(t, t.Schedule(), context, b)
			}
		}
	}()
}

func (h *GenericTaskHandler) runScheduled(t Task, schedule Schedule, ctx context.Context, b broadcaster.Broadcaster) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-schedule.Next():
			vCh := make(chan bool)
			h.verify <- vCh
			if ok := <-vCh; ok {
				slog.Debug(fmt.Sprintf("%s running task %s", h.role, t.Name()))
				go t.Run(ctx, b)
			}
		}
	}
}

func (h *GenericTaskHandler) Verify() <-chan chan bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.verify
}

func (h *GenericTaskHandler) Stop() {
	h.cancel()
}

type GenericTask struct {
	TaskName     string
	TaskType     uint8
	TaskSchedule Schedule
}

func (t *GenericTask) Name() string {
	return t.TaskName
}

func (t *GenericTask) Type() uint8 {
	return t.TaskType
}

func (t *GenericTask) Schedule() Schedule {
	return t.TaskSchedule
}

type IntervalSchedule struct {
	Ticker <-chan time.Time
}

func (*IntervalSchedule) Type() uint8 {
	return ScheduleTypeInterval
}

func (c *IntervalSchedule) Next() <-chan time.Time {
	return c.Ticker
}
