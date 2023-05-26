package agent

// Copyright (C) 2022 by RStudio, PBC

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
	agenttypes "github.com/rstudio/platform-lib/pkg/rsqueue/agent/types"
	"github.com/rstudio/platform-lib/pkg/rsqueue/metrics"
	"github.com/rstudio/platform-lib/pkg/rsqueue/permit"
	"github.com/rstudio/platform-lib/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/pkg/rsqueue/types"
	"github.com/rstudio/platform-lib/pkg/rsqueue/utils"
)

type DefaultAgent struct {
	runner      queue.WorkRunner
	queue       queue.Queue
	runningJobs int64
	mutex       sync.RWMutex
	cEnforcer   *ConcurrencyEnforcer
	extend      time.Duration
	types       queue.QueueSupportedTypes

	logger      types.DebugLogger
	traceLogger types.DebugLogger
	jobLogger   types.DebugLogger

	wrapper metrics.JobLifecycleWrapper

	// Tracks the number of recursion usages in progress
	recursing sync.WaitGroup
	// Tracks the number of running jobs
	running sync.WaitGroup
	// Tracks contexts of running jobs
	runningWork map[permit.Permit]context.Context

	// Notifications channel. This channel is notified when work is done. The
	// agent doesn't need it except for the purpose of flushing and discarding
	// notifications while waiting for available slots.
	msgs <-chan listener.Notification

	notifyTypeWorkComplete uint8

	// Notified when shutting down
	stop chan bool
}

type AgentConfig struct {
	WorkRunner             queue.WorkRunner
	Queue                  queue.Queue
	ConcurrencyEnforcer    *ConcurrencyEnforcer
	SupportedTypes         queue.QueueSupportedTypes
	NotificationsChan      <-chan listener.Notification
	DebugLogger            types.DebugLogger
	TraceLogger            types.DebugLogger
	JobLogger              types.DebugLogger
	NotifyTypeWorkComplete uint8
	JobLifecycleWrapper    metrics.JobLifecycleWrapper
}

func NewAgent(cfg AgentConfig) *DefaultAgent {
	return &DefaultAgent{
		runner:      cfg.WorkRunner,
		queue:       cfg.Queue,
		cEnforcer:   cfg.ConcurrencyEnforcer,
		extend:      5 * time.Second,
		types:       cfg.SupportedTypes,
		runningWork: make(map[permit.Permit]context.Context),
		stop:        make(chan bool),
		msgs:        cfg.NotificationsChan,

		// Optional JobLifecycle wrapper for metrics/tracing.
		wrapper: cfg.JobLifecycleWrapper,

		logger:      cfg.DebugLogger,
		traceLogger: cfg.TraceLogger,
		jobLogger:   cfg.JobLogger,

		notifyTypeWorkComplete: cfg.NotifyTypeWorkComplete,
	}
}

var ErrAgentStopTimeout = errors.New("timeout waiting for queue agent to stop")

var ErrAgentStopped = errors.New("queue agent stopped")

// Waits for jobs that are marked for recursion
func (a *DefaultAgent) waitForJobsWithRecursion(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Every 100ms, check if any jobs require recursion. If not, return true
			if !a.checkForJobsWithRecursion() {
				return true
			}
		case <-timer.C:
			// After timeout, simply return false
			return false
		}
	}
}

func (a *DefaultAgent) checkForJobsWithRecursion() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	for _, ctx := range a.runningWork {
		allowed := ctx.Value(queue.CtxAllowsRecursion)
		if allowed != nil && allowed.(bool) {
			return true
		}
	}
	return false
}

// Stop safely stops the agent.
func (a *DefaultAgent) Stop(timeout time.Duration) error {
	stop := func(done chan struct{}) {
		// Notify caller when done
		defer close(done)

		// Wait for any work marked for potential recursion
		a.waitForJobsWithRecursion(timeout)

		// Wait for any recursing work to stop
		a.recursing.Wait()

		// Stop accepting more work
		a.types.DisableAll()

		// Finish work
		a.running.Wait()

		// Stop agent loop and wait
		a.stop <- true
		<-a.stop
	}

	done := make(chan struct{})
	go stop(done)
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	select {
	case <-done:
		return nil
	case <-timeoutTimer.C:
		return ErrAgentStopTimeout
	}
}

// Wait blocks until there's capacity to run a new job.
// Parameters:
//   - runningJobs int64 - The initial number of running jobs
//   - jobDone chan int64 - Passes the new number of running jobs when any job completes
//
// Returns:
//   - uint64 - The maximum job priority for which we have capacity
func (a *DefaultAgent) Wait(runningJobs int64, jobDone chan int64) uint64 {
	// Flush notifications while the waiting. If the agent is not calling the
	// queue's `Get` function due to no available concurrency slots, then any
	// jobs that complete won't be able to send their work complete notifications
	// in a single-node environment.
	stop := make(chan struct{})
	defer close(stop)
	go a.flushNotifications(stop)

	for {
		capacity, priority := a.cEnforcer.Check(runningJobs)
		if capacity {
			// Return the maximum priority we're able to accept
			return priority

		} else {
			a.traceLogger.Debugf("Concurrency limit reached. Waiting for a job to complete...")
			runningJobs = <-jobDone
			a.traceLogger.Debugf("Job completed. Checking for capacity again.")
		}
	}
}

func (a *DefaultAgent) flushNotifications(stop chan struct{}) {
	for {
		select {
		case <-a.msgs:
		// discard
		case <-stop:
			return
		}
	}
}

func (a *DefaultAgent) Run(notify agenttypes.Notify) {
	defer close(a.stop)

	// When a job completes, we send the new job count to this channel
	jobDone := make(chan int64)

	// When a job completes, we calculate the maximum priority for which we
	// have capacity, and we send the value over this channel
	maxPriorityChan := make(chan uint64)

	retry := 0
	for {
		a.mutex.RLock()
		runningJobs := a.runningJobs
		a.mutex.RUnlock()

		// Wait until we have capacity to run a job. `maxPriority` is the
		// maximum priority we have capacity to handle.
		maxPriority := a.Wait(runningJobs, jobDone)

		// Get a job from the queue (blocks)
		queueWork, err := a.queue.Get(maxPriority, maxPriorityChan, a.types, a.stop)
		if utils.IsSqliteLockError(err) {
			// Do nothing, but sleep a bit to allow competing work through.
			// Only log if tracing.
			a.traceLogger.Debugf("Database lock error during queue Get(): %s", err)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err == ErrAgentStopped {
			// Time to shut down
			return
		} else if err != nil {
			// Otherwise, log the error
			a.logger.Debugf("Waiting for retry after queue Get() returned error: %s", err)
			// Add exponential back-off to avoid spamming
			exp := time.Duration(math.Min(1000, math.Pow(2, float64(retry))))
			time.Sleep((exp * 100) * time.Millisecond)
			retry++
			continue
		}

		retry = 0

		a.traceLogger.Debugf("Grabbed a new job with a maxPriority of '%d', type '%d', address '%s', and permit '%d'", maxPriority, queueWork.WorkType, queueWork.Address, queueWork.Permit)
		if a.jobLogger.Enabled() {
			jsonDst := bytes.Buffer{}
			json.Indent(&jsonDst, queueWork.Work, "", "  ")
			a.jobLogger.Debugf(jsonDst.String())
		}

		// Create a context for the work
		ctx := queue.ContextWithRecursion(context.Background(), queueWork.WorkType, a.getRecurseFn(jobDone))

		// Increment job count
		a.mutex.Lock()
		a.runningJobs += 1
		a.runningWork[queueWork.Permit] = ctx
		a.mutex.Unlock()

		// Track running jobs by adding to the delta before we start
		// the new goroutine. The `runJob` function will call `Done`
		// on the wait group when the job is complete.
		a.running.Add(1)
		// Handle the job and move on to the next job (non-blocking)
		go a.runJob(ctx, queueWork, jobDone, maxPriorityChan, notify)
	}
}

// See below, where `runJob` calls `a.runner.Run`. The `recurse` function
// allows queue work runners to recursively call the queue. For example, we may
// need to get an item from the cache in a job. If we simply get from the
// cache, there's a risk that recursive calls to the queue may result in
// blocking forever. Whenever you need to call the cache or the queue
// recursively from a queue work runner, do so in the context of the
// recurse function:
//
// Example:
//
// ```go
//
//	func (r *SomeWorkRunner) Run(work queue.RecursableWork) error {
//	    // Recursively call the cache for data
//	    recurse(func() {
//	        obj, err := r.cache.Get(spec, &SomeType{})
//	    })
//	}
//
// ```
func (a *DefaultAgent) getRecurseFn(jobDone chan int64) queue.RecurseFunc {
	return func(run func()) {

		// Track recursions
		a.recursing.Add(1)
		defer a.recursing.Done()

		// When the function exits, increase the job count
		defer func() {
			a.mutex.Lock()
			a.runningJobs += 1
			a.mutex.Unlock()
		}()

		// Temporarily decrease job count so we don't block forever
		// during the recursive call.
		a.mutex.Lock()
		a.runningJobs -= 1
		a.traceLogger.Debugf("Recursion function reduced running job count from %d to %d", a.runningJobs+1, a.runningJobs)
		a.mutex.Unlock()

		// Notify the runner that we've freed up capacity due to recursion
		go func() {
			a.mutex.RLock()
			defer a.mutex.RUnlock()
			// Don't block if we're not receiving on the channel
			select {
			case jobDone <- a.runningJobs:
				break
			default:
				break
			}
		}()

		// Do work
		run()
	}
}

// Runs work
// Parameters:
//   - job Work - The work to perform
//   - pt Permit - a Permit to manage the work
//   - jobDone chan int - The channel to notify of the job count when a job completes
//   - maxPriorityChan chan uint64 - A channel on which we notify of capacity changes.
func (a *DefaultAgent) runJob(ctx context.Context, queueWork *queue.QueueWork, jobDone chan int64, maxPriorityChan chan uint64, notify agenttypes.Notify) {
	defer a.running.Done()

	var err error
	if a.wrapper != nil {
		var data interface{}
		ctx, data, err = a.wrapper.Start(ctx, queueWork)
		if err != nil {
			a.traceLogger.Debugf("agent.runJob tracing wrapper start error: %s", err)
		}
		defer a.wrapper.Finish(data)
	}

	// Run the job (blocks)
	func() {

		// Extend the job's heartbeat in the queue periodically until the job completes.
		done := make(chan struct{})
		defer close(done)
		ticker := time.NewTicker(a.extend)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ticker.C:
					// Extend the job visibility
					a.traceLogger.Debugf("Job of type %d visibility timeout needs to be extended: %d\n", queueWork.WorkType, queueWork.Permit)
					err := a.queue.Extend(queueWork.Permit)
					if err != nil {
						a.logger.Debugf("Error extending job for work type %d: %s", queueWork.WorkType, err)
					}
				case <-done:
					return
				}
			}
		}()

		// Actually run the job
		a.traceLogger.Debugf("Running job with type %d, address '%s', and permit '%d'", queueWork.WorkType, queueWork.Address, queueWork.Permit)
		err := a.runner.Run(queue.RecursableWork{
			Work:     queueWork.Work,
			WorkType: queueWork.WorkType,
			Context:  ctx,
		})
		if err != nil {
			a.logger.Debugf("Job type %d: address: %s work: %#v returned error: %s\n", queueWork.WorkType, queueWork.Address, string(queueWork.Work), err)
		}

		// If the work was addressed, record the result
		if queueWork.Address != "" {
			// Note, `RecordFailure` will record the error if err != nil. If
			// err == nil, then it clears any recorded error for the address.
			err = a.queue.RecordFailure(queueWork.Address, err)
			if err != nil {
				a.logger.Debugf("Failed while recording addressed work success/failure: %s\n", err)
			}
		}
	}()

	// Decrement the job count
	a.mutex.Lock()
	a.runningJobs -= 1
	delete(a.runningWork, queueWork.Permit)
	a.mutex.Unlock()

	// Notify the runner that a job is done
	go func() {
		// Don't block if we're not receiving on the channel
		a.mutex.RLock()
		defer a.mutex.RUnlock()
		select {
		case jobDone <- a.runningJobs:
			break
		default:
			break
		}
	}()

	// Delete the job from the queue
	a.traceLogger.Debugf("Deleting job from queue: %d\n", queueWork.Permit)

	if err := a.queue.Delete(queueWork.Permit); err != nil {
		a.logger.Debugf("queue Delete() returned error: %s", err)
	}

	// Notify that work is complete if work is addressed. This must happen after the work has been deleted from the
	// queue.
	if queueWork.Address != "" {
		n := time.Now()
		a.traceLogger.Debugf("Ready to notify of address %s", queueWork.Address)
		notify(agenttypes.NewWorkCompleteNotification(queueWork.Address, a.notifyTypeWorkComplete))
		a.traceLogger.Debugf("Notified of address %s in %d ms", queueWork.Address, time.Now().Sub(n).Nanoseconds()/1000000)
	}

	// Calculate the new max priority we have capacity for
	a.mutex.RLock()
	runningJobs := a.runningJobs
	a.mutex.RUnlock()

	capacity, maxPriority := a.cEnforcer.Check(runningJobs)

	if capacity {
		// Attempt to notify channel. This notification will succeed if the queue Get is
		// waiting for work, but it will not block if the queue is busy
		a.traceLogger.Debugf("Notifying maxPriorityChan of priority %d\n", maxPriority)
		select {
		case maxPriorityChan <- maxPriority:
			a.traceLogger.Debugf("Notified maxPriorityChan of priority %d\n", maxPriority)
		default:
			a.traceLogger.Debugf("Not receiving on maxPriorityChan; skipped notification of priority %d\n", maxPriority)
		}
	}
}
