package metrics

// Copyright (C) 2023 by RStudio, PBC

type Metrics interface {
	QueueNotificationMiss(queue, address string)
}
