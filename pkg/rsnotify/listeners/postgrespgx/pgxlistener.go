package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

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

type PgxIPReporter struct {
	pool *pgxpool.Pool
}

// NewPgxIPReporter creates a new IPReporter.
func NewPgxIPReporter(pool *pgxpool.Pool) *PgxIPReporter {
	return &PgxIPReporter{
		pool: pool,
	}
}

func (p *PgxIPReporter) IP() string {
	ip := "0.0.0.0"
	query := "SELECT inet_client_addr()"
	if p.pool != nil {
		row := p.pool.QueryRow(context.Background(), query)
		pgIp := pgtype.Inet{}
		err := row.Scan(&pgIp)
		if err != nil {
			log.Printf("Unable to determine client IP with inet_client_addr(). %s", err)
			return ip
		}
		if pgIp.IPNet != nil && pgIp.IPNet.IP != nil {
			ip = pgIp.IPNet.IP.String()
		} else {
			log.Printf("Unable to determine client IP with inet_client_addr().")
		}
	} else {
		log.Printf("Invalid pool")
	}
	return ip
}

type PgxListener struct {
	name        string
	pool        *pgxpool.Pool
	conn        *pgxpool.Conn
	cancel      context.CancelFunc
	exit        chan struct{}
	matcher     listener.TypeMatcher
	ip          string
	debugLogger listener.DebugLogger
	ipReporter  listener.IPReporter
}

// NewPgxListener creates a new listener.
// Only intended to be called from a listener factory's `New` method.
func NewPgxListener(name string, pool *pgxpool.Pool, matcher listener.TypeMatcher, debugLogger listener.DebugLogger, iprep listener.IPReporter) *PgxListener {
	return &PgxListener{
		name:        name,
		pool:        pool,
		debugLogger: debugLogger,
		matcher:     matcher,
		ipReporter:  iprep,
	}
}

func (l *PgxListener) IP() string {
	return l.ip
}

func (l *PgxListener) Listen() (items chan listener.Notification, errs chan error, err error) {
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

func (l *PgxListener) wait(ctx context.Context, items chan listener.Notification, errs chan error, ready chan struct{}) (err error) {
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

		l.notify(n, errs, items)
	}
}

func (l *PgxListener) acquire(ready chan struct{}) (err error) {
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
	l.ip = l.ipReporter.IP()

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

func (l *PgxListener) Debugf(msg string, args ...interface{}) {
	if l.debugLogger != nil {
		l.debugLogger.Debugf(msg, args...)
	}
}

func (l *PgxListener) notify(n *pgconn.Notification, errs chan error, items chan listener.Notification) {
	// A notification was received! Unmarshal it into the correct type and send it.
	var input listener.Notification

	// Convert postgres message payload to byte array
	payloadBytes := []byte(n.Payload)

	// Unmarshal the payload to a raw message
	var tmp map[string]*json.RawMessage
	err := json.Unmarshal(payloadBytes, &tmp)
	if err != nil {
		errs <- fmt.Errorf("error unmarshalling raw message: %s", err)
		return
	}

	// Unmarshal request data type
	var dataType uint8
	if tmp[l.matcher.Field()] == nil {
		errs <- fmt.Errorf("message does not contain data type field %s", l.matcher.Field())
		return
	}
	if err = json.Unmarshal(*tmp[l.matcher.Field()], &dataType); err != nil {
		errs <- fmt.Errorf("error unmarshalling message data type: %s", err)
		return
	}
	if l.matcher.Type(dataType) == nil {
		errs <- fmt.Errorf("no matcher type found for %d", dataType)
		return
	}

	// Get an object of the correct type
	input = reflect.New(reflect.ValueOf(l.matcher.Type(dataType)).Elem().Type()).Interface().(listener.Notification)

	// Unmarshal the payload
	err = json.Unmarshal(payloadBytes, input)
	if err != nil {
		errs <- fmt.Errorf("error unmarshalling JSON: %s", err)
		return
	}
	items <- input
}

func (l *PgxListener) Stop() {
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
