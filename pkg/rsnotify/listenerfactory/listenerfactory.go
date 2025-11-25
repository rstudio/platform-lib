package listenerfactory

// Copyright (C) 2022 by RStudio, PBC.

import (
	"github.com/rstudio/platform-lib/v3/pkg/rsnotify/listener"
)

type ListenerFactory interface {
	New(channel string, matcher listener.TypeMatcher) listener.Listener
	Shutdown()
}
