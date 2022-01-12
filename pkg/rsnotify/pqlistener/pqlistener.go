package pqlistener

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/lib/pq"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type PqRetrieveListenerFactory interface {
	NewListener() (*pq.Listener, error)
	IP() (string, error)
}

type PqListener struct {
	name          string
	factory       PqRetrieveListenerFactory
	conn          *pq.Listener
	t             listener.Notification
	cancel        context.CancelFunc
	exit          chan struct{}
	unmarshallers map[uint8]listener.Unmarshaller
	ip            string
	debugLogger   listener.DebugLogger
}

// Only intended to be called from listenerfactory.go's `New` method.
func NewPqListener(name string, i listener.Notification, factory PqRetrieveListenerFactory, unmarshallers map[uint8]listener.Unmarshaller, debugLogger listener.DebugLogger) *PqListener {
	return &PqListener{
		name:          name,
		factory:       factory,
		t:             i,
		debugLogger:   debugLogger,
		unmarshallers: unmarshallers,
	}
}

func (l *PqListener) IP() string {
	return l.ip
}

func (l *PqListener) Listen() (items chan listener.Notification, errs chan error, err error) {
	items = make(chan listener.Notification, listener.MaxChannelSize)
	errs = make(chan error)
	ready := make(chan struct{})

	go func() {
		l.exit = make(chan struct{})
		defer close(l.exit)

		// Create a context that can be cancelled when the server shuts down.
		ctx, cancel := context.WithCancel(context.Background())
		l.cancel = cancel
		defer cancel()

		for {
			if waitErr := l.wait(ctx, items, errs, ready); err != nil {
				errs <- waitErr
			}

			// If we've been signaled to stop, exit. Otherwise, wait a second and retry
			if needExit(ctx) {
				return
			}
		}
	}()

	// Ready is notified when we are actively listening. Wait to return until
	// we have an active listener.
	<-ready
	return
}

func needExit(ctx context.Context) bool {
	tm := time.NewTimer(time.Second)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-tm.C:
	}
	return false
}

func (l *PqListener) wait(ctx context.Context, items chan listener.Notification, errs chan error, ready chan struct{}) (err error) {
	// Set up the connection
	err = l.acquire(ready)
	if err != nil {
		return
	}

	// Listen/Notify event loop
	ch := l.conn.NotificationChannel()
	for {
		select {
		case n := <-ch:
			l.notify(n, l.t, errs, items)
		case <-ctx.Done():
			return
		}
	}
}

func (l *PqListener) acquire(ready chan struct{}) (err error) {
	if l.conn != nil {
		l.conn.Unlisten(l.name)
		l.conn.Close()
		l.conn = nil
	}

	l.ip = ""
	l.conn, err = l.factory.NewListener()
	if err != nil {
		return fmt.Errorf("error acquiring native connection: %s", err)
	}

	// Get the connection's IP
	l.ip, err = l.factory.IP()
	if err != nil {
		return fmt.Errorf("error determining listener IP address: %s", err)
	}

	err = l.conn.Listen(l.name)
	if err != nil {
		return fmt.Errorf("error starting listener: %s", err)
	}

	// If there were no errors, we are now actively listening. Signal that
	// we are ready, or log when reconnecting.
	select {
	case <-ready:
		// Already closed. This means that we are reconnecting
		log.Printf("successfully reconnected listener %s", l.name)
	default:
		// Close the `ready` channel to signal that `Listen()` can return.
		close(ready)
	}

	return
}

func (l *PqListener) Debugf(msg string, args ...interface{}) {
	if l.debugLogger != nil {
		l.debugLogger.Debugf(msg, args...)
	}
}

func (l *PqListener) notify(n *pq.Notification, i interface{}, errs chan error, items chan listener.Notification) {
	// A notification was received! Unmarshal it into the correct type and send it.
	var input listener.Notification
	input = reflect.New(reflect.ValueOf(i).Elem().Type()).Interface().(listener.Notification)
	payloadBytes := []byte(n.Extra)
	err := json.Unmarshal(payloadBytes, input)
	if err != nil {
		errs <- fmt.Errorf("error unmarshalling JSON: %s", err)
		return
	}
	if unmarshaller, ok := l.unmarshallers[input.Type()]; ok {
		err = unmarshaller(input, payloadBytes)
		if err != nil {
			errs <- fmt.Errorf("error unmarshalling with custom unmarshaller: %s", err)
			return
		}
		l.Debugf("Unmarshalled notification of type %d with registered unmarshaller", input.Type())
	}
	items <- input
}

func (l *PqListener) Stop() {
	l.Debugf("Signaling context to cancel listener %s", l.name)
	l.cancel()
	// Wait for stop
	l.Debugf("Waiting for listener %s to stop...", l.name)
	<-l.exit

	// Clean up connection
	if l.conn != nil {
		l.conn.Unlisten(l.name)
		l.conn.Close()
		l.conn = nil
	}

	l.Debugf("Listener %s closed.", l.name)
}
