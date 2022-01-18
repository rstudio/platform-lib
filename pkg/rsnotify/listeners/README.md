# `/pkg/rsnotify/listeners`

This directory contains implementations of `/pkg/rsnotify/listener.Listener`.

Each implementation should have its own `go.mod`. This allows consumers to import
only the listener types they need for their use cases.

Each listener should also implement `/pkg/rsnotify/listenerfactory.ListenerFactory`.
