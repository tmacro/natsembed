# natsembed

Helper module for embedding a [NATS](https://nats.io) server inside your Go process. Start and stop it from code, apply
new settings at runtime, and create in‑process client connections without opening a TCP socket.

---

## Features

* Start with `Start(...)` or `Run(ctx, ...)`.
* `Run` respects context cancelation.
* `Reconfigure(...)` restarts the server with new options and closes tracked in‑process connections.
* `InProcessConnection()` returns an in-process `*nats.Conn`.
* Functional server options

---

## Installation

```bash
go get github.com/tmacro/natsembed
```

---

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/nats-io/nats.go"
    "github.com/tmacro/natsembed"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Run blocks until the context is canceled
    go func() {
        if err := natsembed.Run(ctx,
            natsembed.ServerName("demo"),
            natsembed.JetstreamEnabled(),
        ); err != nil {
            log.Fatal(err)
        }
    }()

    // Wait briefly for startup
    time.Sleep(200 * time.Millisecond)

    nc, err := natsembed.InProcessConnection()
    if err != nil {
        log.Fatal(err)
    }
    defer nc.Drain()

    nc.Subscribe("greet", func(m *nats.Msg) {
        log.Printf("received: %s", string(m.Data))
    })

    nc.Publish("greet", []byte("hello from inside"))

    cancel() // server stops when ctx is done
}
```

---

## API Overview

### Lifecycle

* `Run(ctx context.Context, opts ...ServerOption) error`
  Blocks until `ctx.Done()` then shuts the server down.
* `Start(opts ...ServerOption) error`
  Alias for `Reconfigure`
* `Stop() error`
  Stops the server and closes tracked in‑process connections.
* `Reconfigure(opts ...ServerOption) error`
  Stops the current instance, closes in‑process conns, and starts again with the new options.

### Connections

* `InProcessConnection() (*nats.Conn, error)`
  Returns an in-process client connection.
  Client is set to reconnect automatically to allow for runtime server restarts.

### Options

All configuration is done via functional `ServerOption`s.

| Option                                               | Description                                    |
|------------------------------------------------------|------------------------------------------------|
| `ServerName(name string)`                            | Sets the server name.                          |
| `Host(host string)` / `Port(port int)`               | Client listener address and port.              |
| `ClusterName(name string)`                           | Cluster name.                                  |
| `ClusterHost(host string)` / `ClusterPort(port int)` | Cluster listen address.                        |
| `WithPeer(host string, port int)`                    | Add a single route peer.                       |
| `WithPeers(peers []Peer)`                            | Add multiple peers.                            |
| `JetstreamEnabled()`                                 | Enable JetStream.                              |
| `DebugEnabled()`                                     | Turn on debug logging.                         |
| `TraceEnabled()`                                     | Turn on trace logging.                         |
| `DontListen()`                                       | Do not open a TCP listener (useful for tests). |
| `Nkeys(users []*natsserver.NkeyUser)`                | Configure NKeys auth.                          |
| `Users(users []*natsserver.User)`                    | Configure username/password auth.              |
| `TLS(cfg *tls.Config)`                               | Enable TLS.                                    |
| `StoreDir(dir string)`                               | Persistent store directory (JetStream).        |
| `WithOptions(*natsserver.Options)`                   | Provide your own nats server config.           |
| `WithLogger(natsserver.Logger)`                      | Provide your own logger.                       |

`Peer` is a small struct with `String()` and `URL()` helpers.

```go
p := natsembed.Peer{Host: "n1.example", Port: 4222}
log.Println(p.URL()) // nats://n1.example:4222
```

---

## Clustering Example

```go
natsembed.Start(
    natsembed.ServerName("node-a"),
    natsembed.ClusterName("demo-cluster"),
    natsembed.ClusterHost("127.0.0.1"),
    natsembed.ClusterPort(6222),
    natsembed.WithPeer("127.0.0.1", 6223),
)
```

Start a second process with matching routes to form the cluster.

---

## Shutdown & Reconnects

`InProcessConnection()` enables automatic reconnects with no max attempts and a 2s wait.
When `Stop()` or `Reconfigure()` is called in‑process connections are closed.

---

## License

BSD 3-Clause
