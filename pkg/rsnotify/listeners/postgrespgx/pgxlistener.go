package postgrespgx

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/notifier"
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
		var ipNet net.IPNet
		err := p.pool.QueryRow(context.Background(), query).Scan(&ipNet)
		if err != nil {
			log.Printf("Unable to determine client IP with inet_client_addr(). %s", err)
			return ip
		}
		if ipNet.IP != nil {
			ip = ipNet.IP.String()
		} else {
			log.Printf("Unable to determine client IP with inet_client_addr().")
		}
	} else {
		log.Printf("Invalid pool")
	}
	return ip
}

type PgxListener struct {
	name       string
	pool       *pgxpool.Pool
	conn       *pgxpool.Conn
	cancel     context.CancelFunc
	exit       chan struct{}
	matcher    listener.TypeMatcher
	ip         string
	ipReporter listener.IPReporter
	ipRefresh  time.Duration

	mu sync.RWMutex

	// For caching chunked notifications
	notifyCache map[string]map[int][]byte
}

type PgxListenerArgs struct {
	Name       string
	Pool       *pgxpool.Pool
	Matcher    listener.TypeMatcher
	IpReporter listener.IPReporter
	IpRefresh  time.Duration
}

// NewPgxListener creates a new listener.
// Only intended to be called from a listener factory's `New` method.
func NewPgxListener(args PgxListenerArgs) *PgxListener {
	return &PgxListener{
		name:        args.Name,
		pool:        args.Pool,
		matcher:     args.Matcher,
		ipReporter:  args.IpReporter,
		ipRefresh:   args.IpRefresh,
		notifyCache: make(map[string]map[int][]byte),
	}
}

func (l *PgxListener) IP() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.ip
}

func (l *PgxListener) refreshIP(ctx context.Context) {
	// Set default IP refresh to 5 minutes; disallow negative durations
	if l.ipRefresh <= 0 {
		l.ipRefresh = 5 * time.Minute
	}

	t := time.NewTicker(l.ipRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.mu.Lock()
			l.ip = l.ipReporter.IP()
			l.mu.Unlock()
		}
	}
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

		go l.refreshIP(ctx)

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
	l.mu.Lock()
	defer l.mu.Unlock()

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

func (l *PgxListener) cache(info notifier.ChunkInfo) bool {
	if _, ok := l.notifyCache[info.Guid]; !ok {
		l.notifyCache[info.Guid] = make(map[int][]byte)
	}
	l.notifyCache[info.Guid][info.Chunk] = info.Message
	return len(l.notifyCache[info.Guid]) == info.Count
}

func (l *PgxListener) assemble(guid string, count int) ([]byte, error) {
	result := make([]byte, 0)
	if chunks, ok := l.notifyCache[guid]; !ok {
		return nil, fmt.Errorf("chunks not found in cache")
	} else {
		// Clean up cache upon exit.
		defer delete(l.notifyCache, guid)
		for i := 1; i <= count; i++ {
			if msg, here := chunks[i]; !here {
				return nil, fmt.Errorf("chunk %d missing from cache", i)
			} else {
				result = append(result, msg...)
			}
		}
	}
	return result, nil
}

func (l *PgxListener) notify(n *pgconn.Notification, errs chan error, items chan listener.Notification) {
	// A notification was received! Unmarshal it into the correct type and send it.
	var input listener.Notification

	// Convert postgres message payload to byte array
	payloadBytes := []byte(n.Payload)

	// Check for chunked encoding
	if notifier.IsChunk(payloadBytes) {
		// Parse the chunk
		info, err := notifier.ParseChunk(payloadBytes)
		if err != nil {
			errs <- fmt.Errorf("error decoding chunk: %s", err)
			return
		}

		// Cache the chunk. If `done == true`, then we know that this is the last
		// chunk for this GUID.
		done := l.cache(info)
		if !done {
			// Abort here since we need to receive more chunks to complete the
			// message.
			return
		}

		// Assemble the chunks into the payload
		payloadBytes, err = l.assemble(info.Guid, info.Count)
		if err != nil {
			errs <- fmt.Errorf("error assembling chunks: %s", err)
			return
		}
	}

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
	t, err := l.matcher.Type(dataType)
	if err != nil {
		errs <- err
		return
	}
	if t == nil {
		return
	}

	// Get an object of the correct type
	input = reflect.New(reflect.ValueOf(t).Elem().Type()).Interface().(listener.Notification)

	// Unmarshal the payload
	err = json.Unmarshal(payloadBytes, input)
	if err != nil {
		errs <- fmt.Errorf("error unmarshalling JSON: %s", err)
		return
	}
	items <- input
}

func (l *PgxListener) Stop() {
	slog.Debug(fmt.Sprintf("Signaling context to cancel listener %s", l.name))
	l.cancel()
	// Wait for stop
	slog.Debug(fmt.Sprintf("Waiting for listener %s to stop...", l.name))
	<-l.exit

	// Clean up connection
	if l.conn != nil {
		l.conn.Exec(context.Background(), fmt.Sprintf("UNLISTEN \"%s\"", l.name))
		l.conn.Release()
		l.conn = nil
	}

	slog.Debug(fmt.Sprintf("Listener %s closed.", l.name))
}
