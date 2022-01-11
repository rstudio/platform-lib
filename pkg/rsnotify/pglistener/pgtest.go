package pglistener

/* pgtest.go
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
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

func EphemeralPostgresPool(dbname string) (pool *pgxpool.Pool, err error) {
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
