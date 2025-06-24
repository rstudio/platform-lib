package metrics

// Copyright (C) 2023 by Posit, PBC

type Metrics interface {
	QueueNotificationMiss(queue, address string)
}
