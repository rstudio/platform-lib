package cmd

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"

	// Must import github.com/jackc/pgx/v4/stdlib for sqlx support.
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/rstudio/platform-lib/pkg/rslog/debug"
	"github.com/rstudio/platform-lib/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerfactory"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespgx"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listeners/postgrespq"
	"github.com/rstudio/platform-lib/pkg/rsnotify/listenerutils"
)

var debugLogger debug.DebugLogger

var (
	message string
	driver  string
	connstr string
)

func init() {
	debugLogger = debug.NewDebugLogger(RegionNotify)

	NotifyCmd.Example = `  testnotify notify
  testnotify notify --driver=pq
  testnotify notify --message="hello from pq" --driver=pq --conn-str=postgres://admin:password@localhost/postgres?sslmode=disable
`
	NotifyCmd.Flags().StringVar(&message, "message", "HELO from <driver>", "The message to send.")
	NotifyCmd.Flags().StringVar(&driver, "driver", "local", "The driver to use. Either 'local', 'pgx', or 'pq'.")
	NotifyCmd.Flags().StringVar(&connstr, "conn-str", defaultPgConnStr, "The postgres connection string to use.")

	RootCmd.AddCommand(NotifyCmd)
}

var NotifyCmd = &cobra.Command{
	Use:     "notify",
	Short:   "Command to test notifications",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		if message == "HELO from <driver>" {
			message = fmt.Sprintf("HELO from %s", driver)
		}
		return notify(message)
	},
}

func notify(msg string) error {
	var fact listenerfactory.ListenerFactory
	n := &testNotification{Message: msg}
	switch driver {
	case "local":
		lp := local.NewListenerProvider()
		fact = local.NewListenerFactory(lp)
		go localNotify("main", lp, n)
	case "pgx":
		pool, err := pgxpool.Connect(context.Background(), connstr)
		if err != nil {
			return fmt.Errorf("error connecting to pool: %s", err)
		}
		fact = postgrespgx.NewPgxListenerFactory(pool, debugLogger)
		go pgNotify("main", "pgx", n)
	case "pq":
		pqListenerFactory := &pqlistener{ConnStr: connstr}
		fact = postgrespq.NewPqListenerFactory(pqListenerFactory, debugLogger)
		go pgNotify("main", "postgres", n)
	default:
		return fmt.Errorf("invalid --driver argument value")
	}

	stop := make(chan bool)
	bc, err := broadcaster.NewNotificationBroadcaster(fact.New("main", &testNotification{}), stop)
	if err != nil {
		return fmt.Errorf("error connecting to pool: %s", err)
	}

	ch := bc.Subscribe((&testNotification{}).Type())
	for {
		select {
		case n := <-ch:
			if tn, ok := n.(*testNotification); ok {
				log.Printf("Received notification: %s", tn.Message)
			}
		}
	}
}

// localNotify sends one notification per second with the provided local ListenerProvider.
func localNotify(channelName string, lp *local.ListenerProvider, message *testNotification) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		<-tick.C
		lp.Notify(channelName, message)
	}
}

// pgNotify sends one notification per second with the provided Postgres driver.
// For lib/pq, provide `driverName = postgres`. For `jackc/pgx`, provide
// `driverName = pgx`.
func pgNotify(channelName, driverName string, message *testNotification) {
	// Ensure that the channel name is safe for PostgreSQL
	channelName = listenerutils.SafeChannelName(channelName)

	// Marshal the message to a string.
	msgbytes, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("error marshalling: %s", err)
	}
	msg := string(msgbytes)

	tick := time.NewTicker(time.Second)
	conn, err := sqlx.ConnectContext(context.Background(), driverName, connstr)
	if err != nil {
		log.Fatalf("error connecting for notifications: %s", err)
	}
	defer conn.Close()
	defer tick.Stop()
	for {
		<-tick.C
		query := fmt.Sprintf("select pg_notify('%s', $1)", channelName)
		_, err = conn.ExecContext(context.Background(), query, msg)
		if err != nil {
			log.Fatalf("error sending notification: %s", err)
		}
	}
}
