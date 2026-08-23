# goraft
Implementing raft protocol with golang

# Project Documentation
https://pkg.go.dev/github.com/lwwgo/goraft

# Quick Start

## Prerequisites

- Go 1.21+ (the project tracks a recent Go toolchain; see `go.mod` for the exact version used)
- A Unix-like shell (bash / zsh). macOS and Linux are supported.

## Makefile Targets

The project ships with a `Makefile` that covers the common developer workflows:

| Target       | What it does                                                                 |
| ------------ | ---------------------------------------------------------------------------- |
| `make build` | Compile the binary and place it under `output/bin/goraft`.                   |
| `make lint`  | Run `golangci-lint` over the entire module. The linter is installed on first run (via `go install`) into the user's Go tool directory, so no system-wide write permissions are needed. |
| `make test`  | Run the Go test suite (`go test -race -count=1 ./...`). If no `*_test.go` files exist yet, the command exits cleanly with an informative message. |
| `make demo`  | Build (if needed) and then generate a runnable multi-node cluster layout under `./demo`. Each node gets its own binary (`goraft-nodeN`), config, WAL, snapshot and log directories, plus a single `start.sh` launcher. |
| `make clean` | Remove generated artifacts: both `output/` and `demo/` are deleted. |

All directories (`output/`, `demo/`) and most numeric parameters (`DEMO_NODES`, `DEMO_BASE_PORT`, `DEMO_WITH_LEARNER`, `BIN_NAME`) are overridable on the command line, e.g.:

```bash
make demo DEMO_NODES=5 DEMO_BASE_PORT=9000 DEMO_WITH_LEARNER=1
```

## Run a Raft Demo Cluster

### 1. Generate the cluster layout

```bash
make demo
```

After it completes you will see a layout like:

```
demo/
├── start.sh
├── node0/  (bin/ conf/ log/ snapshot/ wal/)
├── node1/  (bin/ conf/ log/ snapshot/ wal/)
└── node2/  (bin/ conf/ log/ snapshot/ wal/)
```

Every node starts as a follower. A leader is elected automatically after the first election timeout (default timeout is 10s with a small jitter, so expect a leader to appear within ~11s).

### 2. Start the cluster

- Foreground mode (Ctrl+C stops every node):

```bash
cd demo
./start.sh
```

- Daemon mode (nodes keep running after the script exits):

```bash
cd demo
./start.sh -d
# or: ./start.sh daemon
```

### 3. Check status

```bash
cd demo
./start.sh status
```

Sample output:

```
=== goraft demo status ===
  node0        pid=12345  port=1231  ALIVE  role=LEADER
  node1        pid=12346  port=1232  ALIVE  role=FOLLOWER
  node2        pid=12347  port=1233  ALIVE  role=FOLLOWER
```

The `role` is inferred from the last state-change line in each node's log (`running.log` under that node's `log/` directory).

### 4. Stop the cluster

- If you launched in daemon mode, or want to stop a previous run:

```bash
cd demo
./start.sh stop
```

- If you launched in foreground mode, press `Ctrl+C` once and the script traps the signal to kill every node cleanly.

### 5. Inspect logs

Each node writes its own log to its private directory, so you never have to disambiguate output:

```bash
cd demo
tail -f node0/log/running.log      # Raft runtime log for node0
tail -f node0/log/start.stdout.log # stdout/stderr captured on launch
```

The node binaries are also copied per-node and renamed to `goraft-nodeN`,
so `ps aux | grep goraft` directly shows which process belongs to which node.

# Key Flows
## Leader Write Flow
1. Write to local memory
2. Write to local WAL
3. Send logs to other follower nodes concurrently
4. If a majority of peers return append log entry success => mark the log as committed, apply it to the business state machine, advance the applied index, and return succ to the client;
   If a majority of peers return append log entry failure / timeout => roll back the local in-memory log and return fail to the client.

## Follower Write Flow
1. Receive the append log entry request and perform log consistency check
2. If the consistency check fails, delete the last log on this node (in-memory) and return append command failure to the leader
3. Write to local memory
4. Write to local WAL
5. Return append log command success
6. On the next heartbeat request from the leader, check whether `leaderCommitted` is greater than this node's `committedIndex`; if so, mark the log at `leaderCommitted` on this node as committed.

## Leader Crash Handling
### Log Safety Constraint
A leader only commits logs from its current term, but applies all logs that already exist in its local log and have not been applied yet.

### Case 1: The leader has committed and applied a log, returned succ to the client, and crashed before the next heartbeat
In the new leader's term, because of the **log safety constraint**, this old log is no longer committed on its own; instead, the previous log is committed piggybacked when a new log in the new term is committed. However, any old log that has not yet been applied still needs to be applied,
because if the new leader does not apply logs from older terms, the new leader's business state machine would lose meta information that had already been confirmed succ to the client, leading to inconsistencies later on.

### Case 2: The leader has NOT committed or applied the log, has NOT returned succ to the client, and crashed before the next heartbeat
The difference from Case 1 is that at this point the new leader can no longer tell whether the old leader already returned succ to the client. The client has to "take the hit": on timeout, it resends the command and probes whether the previous timed-out command has actually completed. In addition, each client command must carry a unique command identifier (e.g. a distributed ID such as a snowflake ID), and the leader must support idempotency checks.
Both of the cases above assume that a majority of peer nodes have successfully written to their local memory and WAL; other scenarios are simpler and are not enumerated here.

# References
Raft introduction: https://www.cnblogs.com/richaaaard/p/6351705.html

Raft paper (Chinese): https://www.cnblogs.com/linbingdong/p/6442673.html
