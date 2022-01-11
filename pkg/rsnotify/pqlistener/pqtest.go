package pqlistener

/* pqtest.go
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
