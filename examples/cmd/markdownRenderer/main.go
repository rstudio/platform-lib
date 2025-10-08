package main

// Copyright (C) 2022 by RStudio, PBC.

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/handlers"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/notifytypes"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/queuetypes"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/runners"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/storage"
	"github.com/rstudio/platform-lib/v2/examples/cmd/markdownRenderer/store"
	"github.com/rstudio/platform-lib/v2/pkg/rscache"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/broadcaster"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listener"
	"github.com/rstudio/platform-lib/v2/pkg/rsnotify/listeners/local"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent"
	agenttypes "github.com/rstudio/platform-lib/v2/pkg/rsqueue/agent/types"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/impls/database"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/metrics"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/queue"
	"github.com/rstudio/platform-lib/v2/pkg/rsqueue/runnerfactory"
	"github.com/rstudio/platform-lib/v2/pkg/rsstorage/servers/file"
)

// Usage:
///
// To use this test service, simply start the application with:
// `./out/markdownRenderer`
// `./out/markdownRenderer --address localhost:8085`
//
// Then, visit http://localhost:8082 or the address you specified.

const (
	storageCacheTimeout        = time.Minute
	storageWalkTimeout         = time.Minute
	storageChunkSize    uint64 = 1024 * 1024
	fileCacheTimeout           = time.Second * 65
)

// These variables are bound to app flags.
var (
	address string
)

type dupMatcher struct{}

// IsDuplicate helps the cache determine when an AddressedPush returns in error
// due to duplicate work in the queue. When the cache uses the queue to perform,
// and when more than 1 request for the same item is in progress, duplicate work
// gets submitted to the queue. The cache needs to know that a given error from
// the queue means that "duplicate work was submitted" since it is safe to ignore
// these errors.
func (d *dupMatcher) IsDuplicate(err error) bool {
	return err == queue.ErrDuplicateAddressedPush
}

type cacheQueueWrapper struct {
	queue.Queue
}

// AddressedPush is part of the cache's queue interface. Since we're using the
// rsqueue/Queue package for message queuing, we can simply wrap the queue's
// AddressedPush method and hand off the wrapped queue to the cache.
func (q *cacheQueueWrapper) AddressedPush(priority uint64, groupId int64, address string, work rscache.QueueWork) error {
	return q.Queue.AddressedPush(priority, groupId, address, work)
}

func init() {
	flag.StringVar(&address, "address", ":8082", "Server address to use")
}

type leveler struct {
	level slog.Level
}

func (l *leveler) Level() slog.Level {
	return l.level
}

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: &leveler{level: slog.LevelDebug},
	}))
	slog.SetDefault(logger)

	// Parse flags at startup.
	flag.Parse()

	// Trap exit signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Start a new local listener provider and factory. The provider knows how
	// to create a new listener. The factory uses the provider to create new
	// listeners when needed.
	localListenerProvider := local.NewListenerProvider(local.ListenerProviderArgs{})
	localListenerFactory := local.NewListenerFactory(localListenerProvider)
	defer localListenerFactory.Shutdown()

	// Register notification types for which we will listen.
	// Notifications are formatted as JSON. Here we declare that
	// the "NotifyType" field will indicate the message type. The
	// value of the "NotifyType" field should match one of the `notifytype`
	// types we next register.
	matcher := listener.NewMatcher("NotifyType")
	matcher.Register(notifytypes.NotifyTypeQueue, &store.DbQueueNotification{})
	matcher.Register(notifytypes.NotifyTypeWorkComplete, &agenttypes.WorkCompleteNotification{})
	matcher.Register(notifytypes.NotifyTypeChunk, &notifytypes.ChunkNotification{})

	// Create a broadcaster listener to receive notifications. Notifications can only
	// be received by one listener, but a broadcaster (see below) can be used to pass
	// messages to multiple subscribers.
	broadcastListener := localListenerFactory.New(notifytypes.ChannelMessages, matcher)
	defer broadcastListener.Stop()

	// Used to shut down the broadcaster (below) cleanly.
	stopBroadcaster := make(chan bool)
	defer func() {
		// When shutting down, send `true` to the stop channel. This notifies the
		// broadcaster to shut down.
		stopBroadcaster <- true

		// For clean shutdown, wait until the stop channel is closed. When the broadcaster
		// is done shutting down, it will close the channel.
		<-stopBroadcaster
	}()

	// Create a broadcaster to distribute incoming notifications. The broadcaster is
	// capable of receiving messages on a channel and broadcasting them to multiple
	// subscribers. This allows many subscribers to receive a single notification.
	notifyBroadcaster, err := broadcaster.NewNotificationBroadcaster(broadcastListener, stopBroadcaster)
	if err != nil {
		log.Fatalf("Error starting addressed work broadcaster: %s\n", err)
	}

	// Create a store for database interactions. This store implements the
	// `DatabaseQueue` interface in github.com/platform-lib/pkg/rsqueue/impls/database
	// and is used by the database queue implementation. The store also includes the
	// local listener provider to support sending notifications.
	_ = os.Mkdir("data", 0755) // Create a data directory
	exampleStore := store.Open("data/markdownRenderer.sqlite", localListenerProvider)

	// The file storage server supports chunked files, where large files are stored in a
	// directory in "chunks" not larger than a configured size. The chunk waiter is used
	// by the file storage server when processing chunked files to determine when new
	// chunks are ready for reading.
	waiter := storage.NewExampleChunkWaiter(notifyBroadcaster)
	// The notifier is used to connect the storage server with the notification system.
	// When writing chunked files, the notifier is used to notify when new chunks have
	// been written.
	notifier := storage.NewExampleChunkNotifier(exampleStore)
	// Create storage server for storing data on disk.
	fileStorage := file.NewStorageServer(file.StorageServerArgs{
		Dir:          "data",
		ChunkSize:    storageChunkSize,
		Waiter:       waiter,
		Notifier:     notifier,
		Class:        "rendered",
		CacheTimeout: storageCacheTimeout,
		WalkTimeout:  storageWalkTimeout,
	})

	// Start NOTIFY/LISTEN functionality for the queue. Note that the database queue uses its
	// own internal broadcaster so we can always remain subscribed to these notifications.
	queueMessages := notifyBroadcaster.Subscribe(notifytypes.NotifyTypeQueue)
	defer notifyBroadcaster.Unsubscribe(queueMessages)
	workMessages := notifyBroadcaster.Subscribe(notifytypes.NotifyTypeWorkComplete)
	defer notifyBroadcaster.Unsubscribe(workMessages)
	chunkMessages := notifyBroadcaster.Subscribe(notifytypes.NotifyTypeChunk)
	defer notifyBroadcaster.Unsubscribe(chunkMessages)

	// The chunk matcher helps to match a chunk notification with the address of an item
	// in the queue. The queue is receives a notification when a chunked item has chunks
	// ready for reading, and the matcher is used to see if the chunked item notification
	// matches the address of the item the queue is waiting on. Since we don't enforce the
	// implementation of the chunk notifications, we need the matcher to couple the
	// notification to the queue.
	chunkMatcher := &storage.ExampleChunkMatcher{}

	// Start the job queue. The stop channel is used to gracefully shut down the queue.
	stopQueue := make(chan bool)
	defer func() {
		// Signal the queue to stop.
		stopQueue <- true
		// Wait for the queue to gracefully finish in-progress work and shut down.
		<-stopQueue
	}()
	q, err := database.NewDatabaseQueue(database.DatabaseQueueConfig{
		QueueName:              "markdown-renderer",
		NotifyTypeWorkReady:    notifytypes.NotifyTypeQueue,
		NotifyTypeWorkComplete: notifytypes.NotifyTypeWorkComplete,
		NotifyTypeChunk:        notifytypes.NotifyTypeChunk,
		ChunkMatcher:           chunkMatcher,
		CarrierFactory:         &metrics.EmptyCarrierFactory{},
		QueueStore:             exampleStore,
		QueueMsgsChan:          queueMessages,
		WorkMsgsChan:           workMessages,
		ChunkMsgsChan:          chunkMessages,
		StopChan:               stopQueue,
		JobLifecycleWrapper:    &metrics.EmptyJobLifecycleWrapper{},
	})
	if err != nil {
		log.Fatalf("Error starting queue: %s\n", err)
	}

	// Create a wrapper for the queue to make it compatible with the cache.
	wrappedQueue := &cacheQueueWrapper{
		Queue: q,
	}
	// Create a recursion helper for the cache. The recursion helper prevents deadlock when
	// one queue job recursively calls (and waits on) more work from the queue.
	recurser := queue.NewOptionalRecurser(queue.OptionalRecurserConfig{FatalRecurseCheck: false})

	// Create a file-based data cache. The cache attempts to retrieve on-disk data from
	// the `fileStorage` server. If the data does not already exist, it pushes work into
	// the `wrappedQueue` and waits for the work to complete. The work that creates the
	// data is handled by a "runner" registered with the `runnerFactory`, below.
	cache := rscache.NewFileCache(rscache.FileCacheConfig{
		Queue:            wrappedQueue,
		DuplicateMatcher: &dupMatcher{},
		StorageServer:    fileStorage,
		Recurser:         recurser,
		Timeout:          fileCacheTimeout,
	})

	// Track supported types for the queue. This is useful for entering an offline mode,
	// as we may need to disable certain types of work in a specific order. When work
	// types are registered with the runner factory (see `runnerFactory.Add`, below), the
	// types are automatically marked as enabled in `supportedTypes`. This example
	// application does not currently demonstrate disabling types.
	supportedTypes := &queue.DefaultQueueSupportedTypes{}

	// Runner factory for the main queue. The queue uses the runner factory to determine
	// how to accomplish queued work of a specific type.
	runnerFactory := runnerfactory.NewRunnerFactory(runnerfactory.RunnerFactoryConfig{
		SupportedTypes: supportedTypes,
	})

	// Register work runners for the queue/cache.
	rendererRunner := runners.NewRendererRunner(fileStorage)

	// Register runners with the factory. This implicitly marks each work type as
	// enabled in the `supportedTypes` helper, above.
	runnerFactory.Add(queuetypes.WorkTypeMarkdown, rendererRunner)

	// Allow up to 10 simultaneous priority 0 jobs to run on a queue node, and
	// up to 5 priority 1 jobs. Zero (0) is the highest priority.
	concurrencyMap := map[int64]int64{
		0: 10,
		1: 5,
	}
	// The concurrency enforcer helps the queue agent to determine if it has capacity
	// to run a job. It also determines the highest priority work for which it has
	// capacity (remember, higher priority numbers mean lower priority).
	cEnforcer, err := agent.Concurrencies(concurrencyMap, concurrencyMap, []int64{0, 1})
	if err != nil {
		err = fmt.Errorf(err.Error())
		return
	}

	// Create the queue agent that runs the work for the queue. The agent watches
	// for available work. When work is available, the agent creates a work permit
	// and "Pops" the work from the queue *if* the agent has capacity to run the work.
	// After the work is popped, the agent uses the runner factory to find a
	// registered runner and to run the work.
	agentCfg := agent.AgentConfig{
		WorkRunner:             runnerFactory,
		Queue:                  q,
		ConcurrencyEnforcer:    cEnforcer,
		SupportedTypes:         supportedTypes,
		NotificationsChan:      queueMessages,
		NotifyTypeWorkComplete: notifytypes.NotifyTypeWorkComplete,
		JobLifecycleWrapper:    &metrics.EmptyJobLifecycleWrapper{},
	}
	ag := agent.NewAgent(agentCfg)
	go ag.Run(func(n listener.Notification) {
		// Since we don't enforce how notifications are sent, the agent sends a
		// notification to this callback method when work is completed. Here, we
		// simply pass those messages on to the store, which knows how to send
		// notifications.
		err = exampleStore.Notify(notifytypes.ChannelMessages, n)
		if err != nil {
			log.Printf("Error notifying of queue work complete: %s", err)
		}
	})

	// Start HTTP services and listen until the application exits.
	router := mux.NewRouter()
	handler := handlers.NewHttpHandler(address, router, cache)
	ctx, cancel := context.WithCancel(context.Background())
	go handler.Start(ctx)
	// Cancel the handler's context when the application exits for graceful
	// shutdown.
	defer cancel()

	// When a SIGTERM or SIGINT is received, gracefully shut down.
	for {
		select {
		case <-sigCh:
			// Exit on a SIGTERM or SIGINT
			slog.Info("Exiting")
			return
		}
	}
}
