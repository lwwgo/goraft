# goraft

`goraft` is a Raft consensus protocol library written in Go, designed to be imported directly by other Go projects. It provides core capabilities including leader election, log replication, WAL persistence, and snapshot compaction.

## Features

- **Leader Election**: Randomized timeout-based election to avoid split votes
- **Log Replication**: Leader concurrently replicates logs to followers, commits on quorum
- **State Machine Injection**: Business layers implement the `StateMachine` interface to gain consensus guarantees
- **WAL + Snapshot**: Write-Ahead Log for persistence, snapshots for log compaction
- **Follower Redirect**: `GetLeader()` returns the current leader address for client redirection
- **Zero External Dependencies**: Uses only the Go standard library

## Installation

```bash
go get github.com/lwwgo/goraft
```

## Package Layout

```
goraft/
├── types/              # Pure data types and interfaces (zero internal dependencies)
│   └── types.go        # Config, StateMachine, CommandEntry, LogEntry, Peer, RPC messages, etc.
├── wal/                # Write-Ahead Log persistence
│   └── wal.go          # WAL append/load
├── snapshot/           # Snapshot persistence
│   └── snapshotter.go  # Snapshot save/load
├── raft/               # Raft protocol implementation
│   ├── raft.go         # Server struct, InitServer, RPC, lifecycle, exported API
│   ├── election.go     # Leader election logic
│   └── replication.go  # Log replication, heartbeat, commit, apply
├── util/               # Utility functions (RPC helpers, etc.)
│   └── util.go
├── go.mod              # go 1.27, zero external dependencies
└── README.md
```

The dependency chain is clean with no cycles: `raft → wal/snapshot → types`.

## Quick Start

### 1. Implement the StateMachine interface

Business layers implement the `StateMachine` interface. Raft guarantees `Apply` is called in the same order on all nodes:

```go
import "github.com/lwwgo/goraft/types"

type MyStateMachine struct {
    data map[string]string
}

func (sm *MyStateMachine) Apply(op string, data []byte) error {
    // Parse the command and mutate state deterministically
    return nil
}

func (sm *MyStateMachine) Snapshot() ([]byte, error) {
    // Generate a point-in-time snapshot of current state
    return nil, nil
}

func (sm *MyStateMachine) Restore(data []byte) error {
    // Restore state from a snapshot
    return nil
}
```

### 2. Initialize a Raft node

```go
import (
    "time"
    "github.com/lwwgo/goraft/raft"
    "github.com/lwwgo/goraft/types"
)

config := types.Config{
    LocalID:         "127.0.0.1:9001",                          // This node's address
    Peers:           []string{"127.0.0.1:9002", "127.0.0.1:9003"}, // Other cluster members
    WalDir:          "/tmp/raft/wal",                           // WAL directory
    SnapDir:         "/tmp/raft/snap",                          // Snapshot directory
    MaxIndexSpan:    1000,                                      // Snapshot trigger threshold
    StateMachine:    &MyStateMachine{...},                      // Injected state machine
    ElectionTimeout: 3 * time.Second,                           // Election timeout
    HeartbeatInterval: 600 * time.Millisecond,                  // Heartbeat interval (default: ElectionTimeout / 5)
}

node, err := raft.InitServer(config)
if err != nil {
    log.Fatal(err)
}
```

### 3. Start the node

```go
// Option 1: Non-blocking start (recommended; caller manages lifecycle)
node.Start()

// Option 2: Blocking run
// node.Run()
```

### 4. Submit a consensus command

Only the leader can submit commands; followers receive an error:

```go
if !node.IsLeader() {
    leader := node.GetLeader()
    // Redirect client to leader
    return
}

cmd := types.CommandEntry{
    Op:   "set",
    Data: []byte(`{"key":"foo","value":"bar"}`),
}
if err := node.Do(cmd); err != nil {
    log.Fatal(err)
}
```

### 5. Start the Raft RPC listener

Raft internal RPC (election, log replication) requires a TCP listener. You can either let goraft start it automatically or manage it yourself:

```go
// Option 1: Let goraft start it automatically (simple scenarios)
config.AutoStartRPC = true
config.RPCAddr = "127.0.0.1:9001"

// Option 2: Manage it yourself (recommended; can share a port with business RPC)
node.StartRPC("127.0.0.1:9001")
```

## Core API Reference

### `types.Config`

| Field | Type | Description |
|---|---|---|
| `LocalID` | `string` | This node's address |
| `Peers` | `[]string` | Other cluster member addresses |
| `IsLearner` | `bool` | Whether this node is a learner (no voting rights) |
| `Learner` | `string` | Learner node address (if any) |
| `WalDir` | `string` | WAL log directory |
| `SnapDir` | `string` | Snapshot directory |
| `MaxIndexSpan` | `uint64` | Trigger snapshot when log index gap exceeds this |
| `StateMachine` | `StateMachine` | Business state machine implementation |
| `Logger` | `*slog.Logger` | Structured logger; defaults to `slog.Default()` |
| `AutoStartRPC` | `bool` | Whether to auto-start the RPC listener |
| `RPCAddr` | `string` | RPC listen address; uses `LocalID` if empty |
| `ElectionTimeout` | `time.Duration` | Election timeout; defaults to 10s |
| `HeartbeatInterval` | `time.Duration` | Leader heartbeat interval; defaults to `ElectionTimeout / 5` |

### `types.StateMachine` interface

```go
type StateMachine interface {
    Apply(op string, data []byte) error      // Apply a committed log entry
    Snapshot() ([]byte, error)               // Generate a state snapshot
    Restore(data []byte) error               // Restore state from a snapshot
}
```

### `raft.Server` exported methods

| Method | Description |
|---|---|
| `InitServer(types.Config) (*Server, error)` | Initialize a Raft node |
| `StartRPC(addr string) error` | Start the Raft internal RPC service |
| `Start()` | Non-blocking start of election and heartbeat timers |
| `Run()` | Blocking run |
| `Do(types.CommandEntry) error` | Submit a consensus command (leader only) |
| `IsLeader() bool` | Whether this node is the leader |
| `GetLeader() string` | Get the current leader address |
| `GetRole() types.CMRole` | Get current role (Follower/Candidate/Leader/Learner) |

## References

- Raft paper: https://raft.github.io/raft.pdf
- Raft visualization: https://thesecretlivesofdata.com/raft/
