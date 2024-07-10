package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func EphemeralPgxPool(dbname string) (pool *pgxpool.Pool, err error) {
	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return
	}

	config.MaxConns = int32(10)

	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	return
}

func Notify(channelName string, msg []byte, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		"select pg_notify('%s', $1)", channelName)
	_, err := pool.Exec(context.Background(), query, string(msg))
	return err
}

func NotifyRaw(channelName string, msg string, pool *pgxpool.Pool) error {
	query := fmt.Sprintf(
		"select pg_notify('%s', $1)", channelName)
	_, err := pool.Exec(context.Background(), query, msg)
	return err
}
