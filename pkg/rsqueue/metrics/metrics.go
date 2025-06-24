package metrics

// Copyright (C) 2023 by Posit Software, PBC

type Metrics interface {
	QueueNotificationMiss(queue, address string)
}
