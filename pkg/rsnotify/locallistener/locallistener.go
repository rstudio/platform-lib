package locallistener

/* locallistener.go
 *
 * Copyright (C) 2021 by RStudio, PBC
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property of
 * RStudio, PBC and its suppliers, if any. The intellectual and technical
 * concepts contained herein are proprietary to RStudio, PBC and its suppliers
 * and may be covered by U.S. and Foreign Patents, patents in process, and
 * are protected by trade secret or copyright law. Dissemination of this
 * information or reproduction of this material is strictly forbidden unless
 * prior written permission is obtained.
 */

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type LocalListener struct {
	name    string
	stop    chan bool
	items   chan interface{}
	guid    string
	factory *LocalListenerFactory

	// If this channel is non-nil, listening is deferred until this channel
	// is signaled or closed. Currently used in test only.
	deferredStart chan struct{}
}

type LocalListenerFactory struct {
	mutex     *sync.RWMutex
	listeners map[string]*LocalListener

	debugLogger listener.DebugLogger

	notifyTimeout time.Duration
}

func NewLocalListenerFactory() *LocalListenerFactory {
	return NewLocalListenerFactoryWithLogger(nil)
}

func NewLocalListenerFactoryWithLogger(debugLogger listener.DebugLogger) *LocalListenerFactory {
	return &LocalListenerFactory{
		listeners: make(map[string]*LocalListener),
		mutex:     &sync.RWMutex{},

		notifyTimeout: 500 * time.Millisecond,

		debugLogger: debugLogger,
	}
}

// Only intended to be called from listenerfactory.go's `New` method.
func (l *LocalListenerFactory) New(name string) *LocalListener {
	return &LocalListener{
		name:    name,
		guid:    uuid.New().String(),
		factory: l,
	}
}

var ErrNotInFactory = errors.New("a local listener must be created with the LocalListenerFactory.New method")
var ErrNotNotificationType = errors.New("a notification must be of type listener.Notification")

func (l *LocalListener) IP() string {
	return ""
}

func (l *LocalListener) Listen() (chan listener.Notification, chan error, error) {
	if l.factory == nil {
		return nil, nil, ErrNotInFactory
	}

	l.factory.mutex.Lock()
	l.factory.listeners[l.guid] = l
	l.factory.mutex.Unlock()

	l.stop = make(chan bool)
	l.items = make(chan interface{})

	msgs := make(chan listener.Notification, listener.MaxChannelSize)
	errs := make(chan error)
	go l.listen(msgs, errs)

	return msgs, errs, nil
}

func (l *LocalListener) listen(msgs chan listener.Notification, errs chan error) {
	defer func() {
		l.factory.Debugf("Stopping listener...")
		l.items = nil
		close(l.stop)
		l.factory.Debugf("Stopped.")
	}()

	l.wait(msgs, errs, l.stop)
}

// The `start` parameter is for test only
func (l *LocalListener) wait(msgs chan listener.Notification, errs chan error, stop chan bool) {
	// If the `deferredStart` channel is set, we wait to listen until the
	// channel is signaled or closed.
	//
	// This path is currently run only in the unit tests, so I included some
	// `log.Printf` usages are for the benefit of verbose testing output.
	if l.deferredStart != nil {
		// First, wait for the test to notify that it's time to start listening. This
		// gives us a chance to set up a deadlock condition in the test.
		log.Printf("Waiting for test notification to start")
		<-l.deferredStart
		// Next, simulate an unexpected stop by waiting for a stop signal after
		// which we return without ever receiving from the `l.items` channel.
		log.Printf("Proceeding with wait by waiting for stop signal")
		<-stop
		log.Printf("Stopped. Returning without receiving from l.items.")
		return
	}
	for {
		select {
		case <-stop:
			l.factory.Debugf("Stopping wait")
			return
		case i := <-l.items:
			if msg, ok := i.(listener.Notification); ok {
				l.factory.Debugf("Received message: %s. Sending to buffered channel with current size %d", msg.Guid(), len(msgs))
				msgs <- msg
			} else {
				errs <- ErrNotNotificationType
			}
		}
	}
}

func (l *LocalListener) Stop() {
	if l.stop != nil {
		// Signal to stop
		l.factory.Debugf("Signaling stop channel to stop...")
		l.stop <- true

		// Wait for stop
		l.factory.Debugf("Waiting for stop channel to close...")
		<-l.stop
		l.factory.Debugf("Stop channel for %s closed.", l.guid)
		l.stop = nil

		// Remove from factory
		l.factory.mutex.Lock()
		defer l.factory.mutex.Unlock()
		delete(l.factory.listeners, l.guid)
	}
}

func (l *LocalListenerFactory) Debugf(msg string, args ...interface{}) {
	if l.debugLogger != nil {
		l.debugLogger.Debugf(msg, args...)
	}
}

// Only intended to be called from the store conn_base.go's `Notify` method. Please
// use the store method and don't call this directly.
func (l *LocalListenerFactory) Notify(channel string, n interface{}) {
	// Send Notifications
	l.notify(channel, n, nil)
}

func (l *LocalListenerFactory) notify(channel string, n interface{}, prevMissedItems map[string]string) {
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

	l.Debugf("Notify called with type=%s on %d listeners", notifyTxt, len(l.listeners))
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
			l.Debugf("Ready to %s internal items with guid %s for channel %s %s: %+v", notifyTxt, notifyGuid, ll.guid, channel, n)
			func() {
				// It's important to create a ticker and stop it so it doesn't leak. This has the potential to be called
				// thousands of times in a relatively short period on a busy server, so timer leaks can result in
				// significant CPU load.
				timeout := time.NewTimer(l.notifyTimeout)
				defer timeout.Stop()
				select {
				case ll.items <- n:
					l.Debugf("Done with %s in local listener with guid %s for channel %s: %+v", notifyTxt, notifyGuid, channel, n)
				case <-timeout.C:
					l.Debugf("Timeout during %s for listener %s/%s with guid %s for channel %s: %+v", notifyTxt, ll.guid, ll.name, notifyGuid, channel, n)
					// Record the timed-out notification in the `missed` map so we can retry it
					missed[ll.guid] = notifyGuid
				}
			}()
		}
	}

	// Unlock mutex before recursing
	l.mutex.RUnlock()

	// Recurse if there were missed items. Note that since the mutex is now unlocked, any
	// listeners that are in the process of closing can be removed from the factory and
	// no further notifications will be attempted for them. Listeners that remain in the
	// factory after the mutex is again locked by the recursive call to `notify` will be
	// attempted again as needed.
	if len(missed) > 0 {
		l.Debugf("calling l.notify for %+v with guid %s for %d missed items on channel %s", n, notifyGuid, len(missed), channel)
		stopCh := make(chan struct{})
		defer close(stopCh)
		// If logging is enabled, periodically record notifications that are still waiting
		if l.debugLogger != nil && l.debugLogger.Enabled() {
			go func(stop chan struct{}) {
				tick := time.NewTicker(30 * time.Second)
				defer tick.Stop()
				for {
					select {
					case <-tick.C:
						l.Debugf("still waiting on l.notify for +%v with guid %s on channel %s", n, notifyGuid, channel)
					case <-stop:
						return
					}
				}
			}(stopCh)
		}
		l.notify(channel, n, missed)
		l.Debugf("completed calling l.notify for %d missed items with guid %s on channel %s", len(missed), notifyGuid, channel)
	}
}
