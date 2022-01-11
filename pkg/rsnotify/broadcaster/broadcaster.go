package broadcaster

/* broadcaster.go
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
	"log"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type Matcher func(notification listener.Notification) bool

// Given a single NOTIFY/LISTEN channel, acts as an inverse multiplexer and
// streams the events to multiple listening channels
type Broadcaster interface {
	Subscribe(dataType uint8) <-chan listener.Notification
	SubscribeOne(dataType uint8, matcher Matcher) <-chan listener.Notification
	Unsubscribe(ch <-chan listener.Notification)
	IP() string
}

type NotificationBroadcaster struct {
	listener    listener.Listener
	msgs        chan listener.Notification
	errs        chan error
	subscribe   chan Subscription
	unsubscribe chan (<-chan listener.Notification)
	stopSignal  chan bool
}

func NewNotificationBroadcaster(l listener.Listener, stop chan bool) (*NotificationBroadcaster, error) {
	b := &NotificationBroadcaster{
		listener:    l,
		subscribe:   make(chan Subscription),
		unsubscribe: make(chan (<-chan listener.Notification)),
		stopSignal:  stop,
	}
	var err error
	b.msgs, b.errs, err = b.listener.Listen()
	if err != nil {
		return nil, err
	}

	go b.broadcast()
	return b, nil
}

func (b *NotificationBroadcaster) IP() string {
	if b.listener == nil {
		return ""
	}
	return b.listener.IP()
}

func (b *NotificationBroadcaster) broadcast() {
	sinks := make([]Subscription, 0)
	defer close(b.stopSignal)
	for {
		select {
		case <-b.stopSignal:
			b.stop(sinks)
			return
		case msg, more := <-b.msgs:
			if !more {
				b.stop(sinks)
				return
			} // incoming data
			var needFilter bool
			for i, sink := range sinks {
				if msg.Type() == sink.T {
					if sink.One != nil {
						// When the `SubscribeOne` method is used to register a listener, we
						// pass only one message over the channel. We filter via the `sink.One` method
						// which is defined by the call to `SubscribeOne`. If this method returns
						// `true`, then we broadcast the message to the subscribe and immediately
						// unsubscribe the subscriber.
						if sink.One(msg) {
							sink.C <- msg
							// Close the sink channel and mark the sink as used so it is
							// unsubscribed. Setting `needFilter = true` avoids multiple calls
							// to `Filter` by deferring the call to after all messages are
							// passed.
							close(sink.C)
							sinks[i].Used = true
							needFilter = true
						}
					} else {
						sink.C <- msg
					}
				}
			}
			// Remove used sinks, if needed. This removes any subscriptions that were created
			// via `SubscribeOne` that have just received their requested messages.
			if needFilter {
				sinks = Filter(sinks)
			}
		case err, more := <-b.errs:
			if more {
				log.Printf("Received error on queue addressed work notification channel: %s", err)
			}
		case sink := <-b.subscribe:
			sinks = append(sinks, sink)
		case sink := <-b.unsubscribe:
			// remove a sink
			for i, c := range sinks {
				if c.C == sink {
					sinks = append(sinks[:i], sinks[i+1:]...)
					close(c.C)
				}
			}
		}
	}
}

func Filter(sinks []Subscription) []Subscription {
	newSinks := make([]Subscription, 0)
	for _, sink := range sinks {
		if !sink.Used {
			newSinks = append(newSinks, sink)
		}
	}
	return newSinks
}

type Subscription struct {
	// The channel over which messages are passed to the subscriber
	C chan listener.Notification

	// The notification type to which the subscriber subscribes
	T uint8

	// Used for SubscribeOne. A Matcher is a callback function that is used
	// to filter notifications when a subscriber is waiting for a specific single
	// notification. When `One` returns `true`, then the message is passed to the
	// subscriber and the subscription is immediately unsubscribed.
	One Matcher

	// Flagged as `true` when a message has been passed to a `SubscribeOne`
	// subscriber. This property is set so the `Filter` function can remove
	// the subscription.
	Used bool
}

// Subscribe returns a new output channel that will receive all broadcast events.
func (b *NotificationBroadcaster) Subscribe(dataType uint8) <-chan listener.Notification {
	c := make(chan listener.Notification)

	b.subscribe <- Subscription{
		C: c,
		T: dataType,
	}

	return c
}

// SubscribeOne returns a new output channel that will receive one and only one broadcast
// event when the provided Matcher returns `true`. When an event matches the Matcher,
// the event is passed over the output channel and the channel is immediately
// unsubscribed. You should still call `Unsubscribe` with the channel in case an event
// is never received.
func (b *NotificationBroadcaster) SubscribeOne(dataType uint8, matcher Matcher) <-chan listener.Notification {
	c := make(chan listener.Notification)

	b.subscribe <- Subscription{
		C:   c,
		T:   dataType,
		One: matcher,
	}

	return c
}

// Unsubscribe removes a channel from receiving broadcast events. That channel is
// closed as a consequence of unsubscribing.
func (b *NotificationBroadcaster) Unsubscribe(ch <-chan listener.Notification) {
	drainer := func() {
		// It's possible that the broadcaster is still trying to send
		// us events while we're attempting to unsubscribe. Create a
		// dummy drainer which makes sure the broadcaster isn't
		// blocked sending to this channel (which could block other
		// broadcaster activity)
		for {
			_, more := <-ch
			if !more {
				// The channel gets closed as the unsubscribe
				// is processed.
				return
			}
		}
	}
	go drainer()
	b.unsubscribe <- ch
}

// internal stop function that closes the destination channels.
func (b *NotificationBroadcaster) stop(sinks []Subscription) {
	for _, sink := range sinks {
		close(sink.C)
	}
}
