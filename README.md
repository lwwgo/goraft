# goraft
Implementing raft protocol with golang

# Project Documentation
https://pkg.go.dev/github.com/lwwgo/goraft

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
