package postgrespq

// Copyright (C) 2025 by Posit Software, PBC.

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

func EphemeralPqConn(dbname string) (*sqlx.DB, error) {
	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	return sqlx.Connect("postgres", connectionString)
}

func EphemeralPqListener(dbname string) *pq.Listener {
	connectionString := fmt.Sprintf("postgres://admin:password@postgres/%s?sslmode=disable", dbname)
	minReconn := 10 * time.Second
	maxReconn := time.Minute
	l := pq.NewListener(connectionString, minReconn, maxReconn, nil)
	return l
}

func Notify(channelName string, n interface{}, db *sqlx.DB) error {
	msgbytes, err := json.Marshal(n)
	if err != nil {
		return err
	}
	msg := string(msgbytes)
	query := fmt.Sprintf(
		"select pg_notify('%s', $1)", channelName)
	_, err = db.Exec(query, msg)
	return err
}

func NotifyRaw(channelName string, msg string, db *sqlx.DB) error {
	query := fmt.Sprintf(
		"select pg_notify('%s', $1)", channelName)
	_, err := db.Exec(query, msg)
	return err
}
