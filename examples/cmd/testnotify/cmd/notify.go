package cmd

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jmoiron/sqlx"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/spf13/cobra"

	// Must import github.com/jackc/pgx/v4/stdlib for sqlx support.
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listenerfactory"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/postgrespgx"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/postgrespq"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listenerutils"
)

var (
	message string
	driver  string
	connstr string
	filter  string
)

const (
	TestNotificationType = uint8(1)
)

func init() {
	NotifyCmd.Example = `  testnotify notify
  testnotify notify --driver=pq
  testnotify notify --message="hello from pq" --driver=pq --conn-str=postgres://admin:password@localhost/postgres?sslmode=disable
`
	NotifyCmd.Flags().StringVar(&message, "message", "Hello from <driver>", "The message to send.")
	NotifyCmd.Flags().StringVar(&driver, "driver", "local", "The driver to use. Either 'local', 'pgx', or 'pq'.")
	NotifyCmd.Flags().StringVar(&connstr, "conn-str", defaultPgConnStr, "The postgres connection string to use.")
	NotifyCmd.Flags().StringVar(&filter, "filter", "", "Filter for messages matching a pattern.")

	RootCmd.AddCommand(NotifyCmd)
}

var NotifyCmd = &cobra.Command{
	Use:     "notify",
	Short:   "Command to test notifications",
	Example: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		if message == "Hello from <driver>" {
			message = fmt.Sprintf("Hello from %s", driver)
		}
		var rFilter *regexp.Regexp
		var err error
		if filter != "" {
			rFilter, err = regexp.Compile(filter)
			if err != nil {
				return fmt.Errorf("Error parsing filter expression: %s", err)
			}
		}
		drv, fact, prov, err := setup(driver)
		if err != nil {
			return err
		}
		stop := make(chan bool)
		defer close(stop)
		go notify(drv, message, prov, stop)
		return listen(fact, rFilter)
	},
}

func setup(drv string) (string, listenerfactory.ListenerFactory, *local.ListenerProvider, error) {
	var fact listenerfactory.ListenerFactory
	var prov *local.ListenerProvider
	switch drv {
	case "local":
		prov = local.NewListenerProvider(local.ListenerProviderArgs{})
		fact = local.NewListenerFactory(prov)
	case "pgx":
		pool, err := pgxpool.Connect(context.Background(), connstr)
		if err != nil {
			return drv, nil, nil, fmt.Errorf("error connecting to pool: %s", err)
		}
		ipReporter := postgrespgx.NewPgxIPReporter(pool)
		fact = postgrespgx.NewListenerFactory(postgrespgx.ListenerFactoryArgs{Pool: pool, IpReporter: ipReporter})
	case "pq":
		drv = "postgres"
		pqListenerFactory := &pqlistener{ConnStr: connstr}
		fact = postgrespq.NewListenerFactory(postgrespq.ListenerFactoryArgs{Factory: pqListenerFactory})
	default:
		return drv, nil, nil, fmt.Errorf("invalid --driver argument value")
	}
	return drv, fact, prov, nil
}

func notify(drv string, msg string, prov *local.ListenerProvider, stop chan bool) {
	n := &testNotification{
		MessageType: TestNotificationType,
		Message:     msg,
	}
	switch drv {
	case "local":
		localNotify("main", prov, n, stop)
	case "pgx", "postgres":
		pgNotify("main", drv, n, stop)
	}
}

func listen(fact listenerfactory.ListenerFactory, rFilter *regexp.Regexp) error {
	stop := make(chan bool)
	matcher := listener.NewMatcher("MessageType")
	matcher.Register(TestNotificationType, &testNotification{})
	bc, err := broadcaster.NewNotificationBroadcaster(fact.New("main", matcher), stop)
	if err != nil {
		return fmt.Errorf("error connecting to pool: %s", err)
	}

	var ch <-chan listener.Notification
	var listenOnce bool
	if rFilter != nil {
		listenOnce = true
		ch = bc.SubscribeOne(TestNotificationType, func(n listener.Notification) bool {
			var result bool
			if tn, ok := n.(*testNotification); ok {
				result = rFilter.MatchString(tn.Message)
			}
			return result
		})
	} else {
		ch = bc.Subscribe(TestNotificationType)
	}
	for {
		select {
		case n := <-ch:
			if tn, ok := n.(*testNotification); ok {
				log.Printf("Received notification: %s", tn.Message)
				if listenOnce {
					log.Printf("Exiting since expected notification was received.")
					return nil
				}
			}
		}
	}
}

// localNotify sends one notification per second with the provided local ListenerProvider.
func localNotify(channelName string, lp *local.ListenerProvider, message *testNotification, stop chan bool) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			lp.Notify(channelName, message)
		}
	}
}

// pgNotify sends one notification per second with the provided Postgres driver.
// For lib/pq, provide `driverName = postgres`. For `jackc/pgx`, provide
// `driverName = pgx`.
func pgNotify(channelName, driverName string, message *testNotification, stop chan bool) {
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
		select {
		case <-stop:
			return
		case <-tick.C:
			query := fmt.Sprintf("select pg_notify('%s', $1)", channelName)
			_, err = conn.ExecContext(context.Background(), query, msg)
			if err != nil {
				log.Fatalf("error sending notification: %s", err)
			}
		}
	}
}
