package postgrespgx

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

func EphemeralPgxPool(dbname string) (pool *pgxpool.Pool, err error) {
	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return
	}

	config.MaxConns = int32(10)

	pool, err = pgxpool.ConnectConfig(context.Background(), config)
	return
}

func Notify(channelName string, n interface{}, pool *pgxpool.Pool) error {
	msgbytes, err := json.Marshal(n)
	if err != nil {
		return err
	}
	msg := string(msgbytes)
	query := fmt.Sprintf(
		"select pg_notify('%s', $1)", channelName)
	_, err = pool.Exec(context.Background(), query, msg)
	return err
}
