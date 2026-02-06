package local

// Copyright (C) 2022 by RStudio, PBC.

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/listener"
)

type Listener struct {
	name     string
	stop     chan bool
	items    chan interface{}
	guid     string
	provider *ListenerProvider

	// If this channel is non-nil, listening is deferred until this channel
	// is signaled or closed. Currently used in test only.
	deferredStart chan struct{}
}

type ListenerProvider struct {
	mutex         *sync.RWMutex
	listeners     map[string]*Listener
	notifyTimeout time.Duration
}

type ListenerProviderArgs struct {
}

func NewListenerProvider(args ListenerProviderArgs) *ListenerProvider {
	return &ListenerProvider{
		listeners: make(map[string]*Listener),
		mutex:     &sync.RWMutex{},

		notifyTimeout: 500 * time.Millisecond,
	}
}

// Only intended to be called from listenerfactory.go's `New` method.
func (l *ListenerProvider) New(name string) *Listener {
	return &Listener{
		name:     name,
		guid:     uuid.New().String(),
		provider: l,
	}
}

var ErrNotInProvider = errors.New("a local listener must be created with the ListenerProvider.New method")
var ErrNotNotificationType = errors.New("a notification must be of type listener.Notification")

func (l *Listener) IP() string {
	return ""
}

func (l *Listener) Listen() (chan listener.Notification, chan error, error) {
	if l.provider == nil {
		return nil, nil, ErrNotInProvider
	}

	l.provider.mutex.Lock()
	l.provider.listeners[l.guid] = l
	l.provider.mutex.Unlock()

	l.stop = make(chan bool)
	l.items = make(chan interface{})

	msgs := make(chan listener.Notification, listener.MaxChannelSize)
	errs := make(chan error)
	go l.listen(msgs, errs)

	return msgs, errs, nil
}

func (l *Listener) listen(msgs chan listener.Notification, errs chan error) {
	defer func() {
		slog.Debug(fmt.Sprintf("Stopping listener..."))
		l.items = nil
		close(l.stop)
		slog.Debug(fmt.Sprintf("Stopped."))
	}()

	l.wait(msgs, errs, l.stop)
}

// The `start` parameter is for test only
func (l *Listener) wait(msgs chan listener.Notification, errs chan error, stop chan bool) {
	// If the `deferredStart` channel is set, we wait to listen until the
	// channel is signaled or closed.
	//
	// This path is currently run only in the unit tests, so I included some
	// `slog.Debug` usages are for the benefit of verbose testing output.
	if l.deferredStart != nil {
		// First, wait for the test to notify that it's time to start listening. This
		// gives us a chance to set up a deadlock condition in the test.
		slog.Debug("Waiting for test notification to start")
		<-l.deferredStart
		// Next, simulate an unexpected stop by waiting for a stop signal after
		// which we return without ever receiving from the `l.items` channel.
		slog.Debug("Proceeding with wait by waiting for stop signal")
		<-stop
		slog.Debug("Stopped. Returning without receiving from l.items.")
		return
	}
	for {
		select {
		case <-stop:
			slog.Debug(fmt.Sprintf("Stopping wait"))
			return
		case i := <-l.items:
			if msg, ok := i.(listener.Notification); ok {
				slog.Debug(fmt.Sprintf("Received message: %s. Sending to buffered channel with current size %d", msg.Guid(), len(msgs)))
				msgs <- msg
			} else {
				errs <- ErrNotNotificationType
			}
		}
	}
}

func (l *Listener) Stop() {
	if l.stop != nil {
		// Signal to stop
		slog.Debug(fmt.Sprintf("Signaling stop channel to stop..."))
		l.stop <- true

		// Wait for stop
		slog.Debug(fmt.Sprintf("Waiting for stop channel to close..."))
		<-l.stop
		slog.Debug(fmt.Sprintf("Stop channel for %s closed.", l.guid))
		l.stop = nil

		// Remove from provider
		l.provider.mutex.Lock()
		defer l.provider.mutex.Unlock()
		delete(l.provider.listeners, l.guid)
	}
}

// Only intended to be called from the store conn_base.go's `Notify` method. Please
// use the store method and don't call this directly.
func (l *ListenerProvider) Notify(channel string, n interface{}) {
	// Send Notifications
	l.notify(channel, n, nil)
}

func (l *ListenerProvider) notify(channel string, n interface{}, prevMissedItems map[string]string) {
	l.mutex.RLock()

	// Map that contains a `true` value keyed by the listener GUID for
	// all listeners that missed an attempted notification due to a timeout.
	missed := make(map[string]string)

	// Create a unique guid for this notification. This allows us to more easily track
	// (for logging) the number of renotification attempts for a particular notification
	notifyGuid := uuid.New().String()

	// For logging
	notifyTxt := "notify"
	if prevMissedItems != nil {
		notifyTxt = "renotify"
	}

	slog.Debug(fmt.Sprintf("Notify called with type=%s on %d listeners", notifyTxt, len(l.listeners)))
	for _, ll := range l.listeners {
		var needNotify bool
		if prevMissedItems == nil {
			// If missedItems == nil, then we need to notify all listeners
			needNotify = true
		} else {
			// Otherwise, we don't need to notify all listeners, so use the
			// missed items map to figure out which ones to notify
			notifyGuid, needNotify = prevMissedItems[ll.guid]
		}
		if needNotify && ll.items != nil && ll.name == channel {
			// There's a chance that `ll.items` could be non-nil, but not receiving. Timeout
			// to prevent deadlock, but keep trying until we're sure that the listener is
			// closed.
			slog.Debug(fmt.Sprintf("Ready to %s internal items with guid %s for channel %s %s: %+v", notifyTxt, notifyGuid, ll.guid, channel, n))
			func() {
				// It's important to create a ticker and stop it so it doesn't leak. This has the potential to be called
				// thousands of times in a relatively short period on a busy server, so timer leaks can result in
				// significant CPU load.
				timeout := time.NewTimer(l.notifyTimeout)
				defer timeout.Stop()
				select {
				case ll.items <- n:
					slog.Debug(fmt.Sprintf("Done with %s in local listener with guid %s for channel %s: %+v", notifyTxt, notifyGuid, channel, n))
				case <-timeout.C:
					slog.Debug(fmt.Sprintf("Timeout during %s for listener %s/%s with guid %s for channel %s: %+v", notifyTxt, ll.guid, ll.name, notifyGuid, channel, n))
					// Record the timed-out notification in the `missed` map so we can retry it
					missed[ll.guid] = notifyGuid
				}
			}()
		}
	}

	// Unlock mutex before recursing
	l.mutex.RUnlock()

	// Recurse if there were missed items. Note that since the mutex is now unlocked, any
	// listeners that are in the process of closing can be removed from the provider and
	// no further notifications will be attempted for them. Listeners that remain in the
	// provider after the mutex is again locked by the recursive call to `notify` will be
	// attempted again as needed.
	if len(missed) > 0 {
		slog.Debug(fmt.Sprintf("calling l.notify for %+v with guid %s for %d missed items on channel %s", n, notifyGuid, len(missed), channel))
		stopCh := make(chan struct{})
		defer close(stopCh)
		// If logging is enabled, periodically record notifications that are still waiting
		go func(stop chan struct{}) {
			tick := time.NewTicker(30 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					slog.Debug(fmt.Sprintf("still waiting on l.notify for +%v with guid %s on channel %s", n, notifyGuid, channel))
				case <-stop:
					return
				}
			}
		}(stopCh)
		l.notify(channel, n, missed)
		slog.Debug(fmt.Sprintf("completed calling l.notify for %d missed items with guid %s on channel %s", len(missed), notifyGuid, channel))
	}
}
