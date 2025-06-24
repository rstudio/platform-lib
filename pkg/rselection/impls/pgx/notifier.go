package pgxelection

// Copyright (C) 2025 by Posit Software, PBC

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxChannelLen = 63

type PgNotifier interface {
	Notify(channel string, msgBytes []byte) error
}

type PgxPgNotifier struct {
	pool *pgxpool.Pool
}

type PgxPgNotifierConfig struct {
	Pool *pgxpool.Pool
}

func NewPgxPgNotifier(cfg PgxPgNotifierConfig) *PgxPgNotifier {
	return &PgxPgNotifier{pool: cfg.Pool}
}

// Makes a channel name safe. PostgreSQL enforces a max channel name size of 64 bytes.
func safeChannelName(channel string) string {
	if len(channel) > MaxChannelLen {
		h := md5.New()
		_, err := h.Write([]byte(channel))
		if err != nil {
			// If there was a hashing error, just truncate
			channel = channel[0:MaxChannelLen]
		} else {
			channel = hex.EncodeToString(h.Sum(nil))
		}
	}
	return channel
}

func (p *PgxPgNotifier) Notify(channel string, msgBytes []byte) error {
	msg := string(msgBytes)

	conn, err := p.pool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		conn.Release()
	}()

	// Ensure that the channel name is safe for PostgreSQL
	channel = safeChannelName(channel)
	query := fmt.Sprintf("select pg_notify('%s', $1)", channel)
	_, err = conn.Exec(context.Background(), query, msg)
	return err
}
