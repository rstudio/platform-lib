# `/pkg/rsnotify/listeners`

This directory contains implementations of `/pkg/rsnotify/listener.Listener`.

All implementations are part of the root module
(`github.com/rstudio/platform-lib/v4`). Import only the listener packages you
need for your use case.

Each listener should also implement `/pkg/rsnotify/listenerfactory.ListenerFactory`.
