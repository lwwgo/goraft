// Package types defines the core data types and interfaces shared across
// goraft sub-packages. It has zero internal dependencies so it can be safely
// imported by storage, server, and downstream consumers.
package types

import (
	"log/slog"
	"reflect"
	"time"
)

// CMRole represents the role of a Raft node.
type CMRole int

const (
	Follower CMRole = iota
	Candidate
	Leader
	// Learner does not participate in voting and is not counted in quorum.
	// It only receives append-log requests from the leader and generates snapshots.
	Learner
	Dead
)

func (s CMRole) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Learner:
		return "learner"
	case Dead:
		return "dead"
	default:
		return "unreachable"
	}
}

// MessageType identifies the kind of Raft RPC message.
type MessageType int

const (
	MsgVote MessageType = iota
	MsgVoteResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgAppendLog
	MsgAppendLogResp
)

func (mt MessageType) String() string {
	switch mt {
	case MsgVote:
		return "msgVote"
	case MsgVoteResp:
		return "msgVoteResp"
	case MsgHeartbeat:
		return "msgHeartBeat"
	case MsgHeartbeatResp:
		return "msgHeartbeatResp"
	case MsgAppendLog:
		return "msgAppendLog"
	case MsgAppendLogResp:
		return "msgAppendLogResp"
	default:
		return "unsupported"
	}
}

// Peer represents a Raft cluster member.
type Peer struct {
	Addr string
	Role CMRole
}

func (p Peer) Empty() bool {
	return reflect.DeepEqual(p, Peer{})
}

func (p Peer) Equal(x Peer) bool {
	return p.Addr == x.Addr
}

// CommandEntry is the business command carried by a Raft log entry.
// Op identifies the operation type (e.g. "mkdir" / "create_file"),
// Data is the arbitrary payload (usually JSON-serialized bytes),
// Size is an optional size hint. The business layer receives and parses
// it via StateMachine.Apply.
type CommandEntry struct {
	Op   string
	Data []byte
	Size int
}

// StateMachine is the interface that business layers must implement.
// Raft guarantees that Apply is executed in the same order on all nodes,
// ensuring state machine consistency.
type StateMachine interface {
	// Apply applies a committed log command to the local state machine.
	Apply(op string, data []byte) error
	// Snapshot generates a snapshot of the current state (for log compaction).
	Snapshot() ([]byte, error)
	// Restore recovers the state machine from a snapshot.
	Restore(data []byte) error
}

// LogEntry is a single entry in the Raft log.
type LogEntry struct {
	Command CommandEntry
	Term    uint64
	Index   uint64
}

// Config is the configuration for a Raft node.
type Config struct {
	// LocalID is this node's address, also used as the Raft RPC listen address.
	LocalID string
	// IsLearner indicates whether this node is a learner (no voting rights).
	IsLearner bool
	// Peers is the list of other node addresses in the cluster.
	Peers []string
	// Learner is the learner node address (if any).
	Learner string
	// WalDir is the WAL log directory.
	WalDir string
	// SnapDir is the snapshot directory.
	SnapDir string
	// MaxIndexSpan triggers a snapshot when the gap between applied index
	// and the first log index exceeds this value.
	MaxIndexSpan uint64

	// StateMachine is the business state machine implementation.
	// If nil, a no-op default is used.
	StateMachine StateMachine
	// Logger is the structured logger. If nil, slog.Default() is used.
	Logger *slog.Logger
	// AutoStartRPC, when true, makes InitServer automatically start the
	// Raft RPC listener. Library consumers usually set this to false and
	// manage the RPC server themselves via StartRPC.
	AutoStartRPC bool
	// RPCAddr is the listen address when AutoStartRPC is true.
	// Empty means use LocalID.
	RPCAddr string
	// ElectionTimeout is the election timeout. Defaults to 10s.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often the leader sends heartbeats.
	// Defaults to ElectionTimeout / 5 if zero. Should be much smaller
	// than ElectionTimeout to prevent spurious elections.
	HeartbeatInterval time.Duration
}

// SnapshotMetadata holds the index and term at which a snapshot was taken.
type SnapshotMetadata struct {
	Index uint64
	Term  uint64
}

// Snapshot is a point-in-time state of the business state machine.
type Snapshot struct {
	Data     []byte
	Metadata SnapshotMetadata
}

// RequestVote is the vote-request RPC message.
type RequestVote struct {
	Type MessageType
	// Term is the candidate's current term.
	Term uint64
	// CandidateID is the requesting node.
	CandidateID Peer
	// LastTerm is the term of the candidate's last log entry.
	LastTerm uint64
	// LastIndex is the index of the candidate's last log entry.
	LastIndex uint64
}

// ResponseVote is the vote-response RPC message.
type ResponseVote struct {
	// Term is the responder's current term.
	Term uint64
	// VoteGranted is true when the candidate receives the vote.
	VoteGranted bool
}

// RequestAppend is the append-log / heartbeat RPC message.
type RequestAppend struct {
	Type            MessageType
	Term            uint64
	LeaderID        Peer
	PreLogIndex     uint64
	PreLogTerm      uint64
	Entries         []LogEntry
	LeaderCommitted uint64
}

// ResponseAppend is the append-log / heartbeat RPC response.
type ResponseAppend struct {
	Term    uint64
	Success bool
}
