package pglistener

/* pglistener.go
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"reflect"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/rstudio/platform-lib/pkg/rsnotify/listener"
)

type PostgresListener struct {
	name          string
	pool          *pgxpool.Pool
	conn          *pgxpool.Conn
	t             listener.Notification
	cancel        context.CancelFunc
	exit          chan struct{}
	unmarshallers map[uint8]listener.Unmarshaller
	ip            string
	debugLogger   listener.DebugLogger
}

// Only intended to be called from listenerfactory.go's `New` method.
func NewPostgresListener(name string, i listener.Notification, pool *pgxpool.Pool, unmarshallers map[uint8]listener.Unmarshaller, debugLogger listener.DebugLogger) *PostgresListener {
	return &PostgresListener{
		name:          name,
		pool:          pool,
		t:             i,
		debugLogger:   debugLogger,
		unmarshallers: unmarshallers,
	}
}

func (l *PostgresListener) IP() string {
	return l.ip
}

func (l *PostgresListener) Listen() (items chan listener.Notification, errs chan error, err error) {
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
			if err := l.wait(ctx, items, errs, ready); err != nil {
				errs <- err
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

func (l *PostgresListener) wait(ctx context.Context, items chan listener.Notification, errs chan error, ready chan struct{}) (err error) {
	// Set up the connection
	err = l.acquire(ready)
	if err != nil {
		return
	}

	// Listen/Notify event loop
	for {
		var n *pgconn.Notification
		n, err = l.conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// Timeout errors are safe to check if context is done
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				err = nil
			}

			if errors.Is(err, context.Canceled) {
				err = nil
			}

			return
		}

		l.notify(n, l.t, errs, items)
	}
}

func (l *PostgresListener) acquire(ready chan struct{}) (err error) {
	if l.conn != nil {
		l.conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN \"%s\"", l.name))
		l.conn.Release()
		l.conn = nil
	}

	l.ip = ""
	l.conn, err = l.pool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("error acquiring native connection: %s", err)
	}

	// Get the connection's IP
	ipQuery := "SELECT inet_client_addr()"
	row := l.conn.QueryRow(context.Background(), ipQuery)
	pgIp := pgtype.Inet{}
	err = row.Scan(&pgIp)
	if err != nil {
		return
	}
	if pgIp.IPNet != nil && pgIp.IPNet.IP != nil {
		l.ip = pgIp.IPNet.IP.String()
	} else {
		log.Printf("Unable to determine client IP with inet_client_addr().")
		l.ip = "0.0.0.0"
	}

	_, err = l.conn.Exec(context.Background(), fmt.Sprintf("LISTEN \"%s\"", l.name))
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

func (l *PostgresListener) Debugf(msg string, args ...interface{}) {
	if l.debugLogger != nil {
		l.debugLogger.Debugf(msg, args...)
	}
}

func (l *PostgresListener) notify(n *pgconn.Notification, i interface{}, errs chan error, items chan listener.Notification) {
	// A notification was received! Unmarshal it into the correct type and send it.
	var input listener.Notification
	input = reflect.New(reflect.ValueOf(i).Elem().Type()).Interface().(listener.Notification)
	payloadBytes := []byte(n.Payload)
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

func (l *PostgresListener) Stop() {
	l.Debugf("Signaling context to cancel listener %s", l.name)
	l.cancel()
	// Wait for stop
	l.Debugf("Waiting for listener %s to stop...", l.name)
	<-l.exit

	// Clean up connection
	if l.conn != nil {
		l.conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN \"%s\"", l.name))
		l.conn.Release()
		l.conn = nil
	}

	l.Debugf("Listener %s closed.", l.name)
}