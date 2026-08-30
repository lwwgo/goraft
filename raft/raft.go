// Package raft implements the Raft consensus protocol.
// It contains the Server struct and all its methods (election, log
// replication, RPC handlers). Pure data types live in goraft/types,
// WAL storage lives in goraft/wal, and snapshot storage lives in
// goraft/snapshot.
package raft

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lwwgo/goraft/snapshot"
	"github.com/lwwgo/goraft/types"
	"github.com/lwwgo/goraft/util"
	"github.com/lwwgo/goraft/wal"
)

// nopStateMachine is a no-op StateMachine used when none is injected.
type nopStateMachine struct{}

func (nopStateMachine) Apply(op string, data []byte) error { return nil }
func (nopStateMachine) Snapshot() ([]byte, error)          { return nil, nil }
func (nopStateMachine) Restore(data []byte) error          { return nil }

// Server is a Raft node.
type Server struct {
	// LocalID is this node.
	LocalID types.Peer
	// Peers are the other nodes in the cluster.
	Peers []types.Peer

	// RpcServer is the Raft internal RPC server (election, log replication).
	RpcServer *rpc.Server

	// Role is the current node role.
	Role types.CMRole
	// Term is the current term.
	Term uint64
	// Logs is the in-memory log entries, pending persistence.
	Logs []types.LogEntry
	// VotedFor is the node voted for in the current term. Empty means not voted.
	VotedFor types.Peer
	// CommittedIndex is the index of the highest committed log entry.
	CommittedIndex uint64
	// AppliedIndex is the index of the highest log entry applied to the state machine.
	AppliedIndex uint64
	// ElectionTimeStart is the last election timeout reset time.
	ElectionTimeStart time.Time
	// TimeOut is the election timeout interval.
	TimeOut time.Duration
	// TimeOutRandomFactor adds jitter to election timeout.
	TimeOutRandomFactor float64
	// MuLock protects node state.
	MuLock sync.Mutex
	// writeMu serializes WriteLog calls to prevent index corruption.
	writeMu sync.Mutex

	// WAL is the Write-Ahead Log storage.
	WAL *wal.WAL
	// Snap is the snapshot storage.
	Snap *snapshot.Snapshotter
	// stateMachine is the injected business state machine.
	stateMachine types.StateMachine
	// logger is the structured logger.
	logger *slog.Logger
	// leaderAddr is the current leader address (known via heartbeat, empty if unknown).
	leaderAddr string
	// MaxIndexSpan triggers a snapshot when AppliedIndex - firstLogIndex exceeds this.
	MaxIndexSpan uint64
	// IsSnaping indicates whether a snapshot is in progress.
	IsSnaping bool
	// HeartbeatInterval is how often the leader sends heartbeats.
	HeartbeatInterval time.Duration

	// NextIndex tracks the next log index to send to each follower (leader only).
	NextIndex map[string]uint64
}

// InitServer initializes a Raft node from the given configuration.
func InitServer(conf types.Config) (*Server, error) {
	peers := make([]types.Peer, 0, len(conf.Peers)+1)
	for _, addr := range conf.Peers {
		// Peers means "other nodes in the cluster"; if self is accidentally
		// included, remove it. Self-RPC would not get votes/copies and would
		// inflate the quorum denominator, preventing leader election.
		if addr == conf.LocalID {
			continue
		}
		peers = append(peers, types.Peer{Addr: addr})
	}
	if len(conf.Learner) > 0 && conf.Learner != conf.LocalID {
		peers = append(peers, types.Peer{Addr: conf.Learner, Role: types.Learner})
	}

	role := types.Follower
	if conf.IsLearner {
		role = types.Learner
	}

	// State machine: use no-op if not injected.
	sm := conf.StateMachine
	if sm == nil {
		sm = nopStateMachine{}
	}
	// Logger: use slog.Default() if not injected.
	logger := conf.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// Election timeout: default 10s.
	electionTimeout := conf.ElectionTimeout
	if electionTimeout <= 0 {
		electionTimeout = 10 * time.Second
	}
	// Heartbeat interval: default ElectionTimeout / 5 to stay well below
	// the randomized election window (electionTimeout * [1, 1+factor]).
	heartbeatInterval := conf.HeartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = electionTimeout / 5
	}

	s := &Server{
		LocalID:             types.Peer{Addr: conf.LocalID},
		Peers:               peers,
		Role:                role,
		Term:                0,
		Logs:                make([]types.LogEntry, 0),
		VotedFor:            types.Peer{},
		CommittedIndex:      0,
		AppliedIndex:        0,
		ElectionTimeStart:   time.Now(),
		TimeOut:             electionTimeout,
		TimeOutRandomFactor: 0.5,
		NextIndex:           make(map[string]uint64, len(peers)),
		WAL:                 wal.New(0, 0, conf.WalDir),
		Snap:                snapshot.New(0, 0, conf.SnapDir),
		stateMachine:        sm,
		logger:              logger,
		MaxIndexSpan:        conf.MaxIndexSpan,
		HeartbeatInterval:   heartbeatInterval,
	}

	// Load snapshot and restore state machine.
	isExist := util.PathIsExist(s.Snap.WorkPath)
	if !isExist {
		log.Printf("%s does not exist, mkdir it\n", s.Snap.WorkPath)
		os.Mkdir(s.Snap.WorkPath, os.ModePerm)
	}
	files, err := os.ReadDir(s.Snap.WorkPath)
	if err != nil {
		return nil, err
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}
		filepath := path.Join(s.Snap.WorkPath, fileInfo.Name())
		if strings.Contains(fileInfo.Name(), ".snap") {
			snapshot, err := s.Snap.Load(filepath)
			if err != nil {
				return nil, err
			}
			s.AppliedIndex = snapshot.Metadata.Index
			s.CommittedIndex = snapshot.Metadata.Index
			s.Term = snapshot.Metadata.Term
			if err := sm.Restore(snapshot.Data); err != nil {
				return nil, fmt.Errorf("restore state machine from snapshot failed: %w", err)
			}
			logger.Info("reload snapshot file success", "file", filepath, "apply_index", s.AppliedIndex, "term", s.Term)
		}
	}

	// Load WAL into memory.
	isExist = util.PathIsExist(s.WAL.WorkPath)
	if !isExist {
		log.Printf("%s does not exist, mkdir it\n", s.WAL.WorkPath)
		os.Mkdir(s.WAL.WorkPath, os.ModePerm)
	}
	files, err = os.ReadDir(s.WAL.WorkPath)
	if err != nil {
		return nil, err
	}

	// Merge all WAL entries into a single slice, independent of file load order.
	var allEntries []types.LogEntry
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}
		filepath := path.Join(s.WAL.WorkPath, fileInfo.Name())
		if strings.Contains(fileInfo.Name(), ".wal") {
			logEntries, err := s.WAL.Load(filepath, s.AppliedIndex)
			if err != nil {
				return nil, err
			}
			allEntries = append(allEntries, logEntries...)
			log.Printf("reload wal file:%s succ, length of logs:%d\n", filepath, len(logEntries))
			os.Remove(filepath)
		}
	}

	if len(allEntries) > 0 {
		// Sort by Index ascending; for same Index, larger Term wins.
		sort.Slice(allEntries, func(i, j int) bool {
			if allEntries[i].Index != allEntries[j].Index {
				return allEntries[i].Index < allEntries[j].Index
			}
			return allEntries[i].Term > allEntries[j].Term
		})
		// Deduplicate same-Index entries (larger Term first due to sort).
		unique := make([]types.LogEntry, 0, len(allEntries))
		for i, e := range allEntries {
			if i == 0 || e.Index != unique[len(unique)-1].Index {
				unique = append(unique, e)
			}
		}
		s.Logs = append(s.Logs, unique...)
		if last := s.Logs[len(s.Logs)-1].Term; last > s.Term {
			s.Term = last
		}
		// Replay committed-but-unapplied logs to the state machine (incremental after snapshot).
		for _, e := range unique {
			if e.Index > s.AppliedIndex {
				if err := sm.Apply(e.Command.Op, e.Command.Data); err != nil {
					return nil, fmt.Errorf("replay log index %d failed: %w", e.Index, err)
				}
				s.AppliedIndex = e.Index
				s.CommittedIndex = e.Index
			}
		}
		logger.Info("wal replayed to state machine", "replayed", len(unique), "applied_index", s.AppliedIndex)
	}

	if len(s.Logs) > 0 {
		s.WAL.Term = s.Logs[len(s.Logs)-1].Term
		s.WAL.Index = s.Logs[len(s.Logs)-1].Index
		s.WAL.SetPath()
	}

	lastIdx := s.getLastLogIndex()
	for _, peer := range peers {
		s.NextIndex[peer.Addr] = lastIdx + 1
	}
	logger.Info("raft server initialized", "next_index", s.NextIndex, "role", s.Role.String())

	// Auto-start RPC listener if configured.
	if conf.AutoStartRPC {
		if err := s.StartRPC(conf.RPCAddr); err != nil {
			return nil, fmt.Errorf("start raft rpc failed: %w", err)
		}
	}

	return s, nil
}

// StartRPC starts the Raft internal RPC service (election, log replication).
// If addr is empty, LocalID.Addr is used as the listen address.
// Business layers typically call this to manage the RPC lifecycle themselves,
// possibly sharing a port with their own business RPC.
func (s *Server) StartRPC(addr string) error {
	if addr == "" {
		addr = s.LocalID.Addr
	}
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("Server", s); err != nil {
		return fmt.Errorf("register raft rpc service failed: %w", err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen raft rpc failed on %s: %w", addr, err)
	}
	s.logger.Info("raft rpc server started", "addr", addr)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				s.logger.Error("raft rpc accept failed", "error", err)
				return
			}
			go rpcServer.ServeConn(conn)
		}
	}()
	return nil
}

// Start non-blockingly starts the election and heartbeat timers.
func (s *Server) Start() {
	s.logger.Info("raft node starting", "role", s.Role.String(), "local_id", s.LocalID.Addr)
	go s.RunElectionTimer()
	go s.RunHeartbeatTimer()
}

// Run blocks running Raft until the process exits.
func (s *Server) Run() {
	s.Start()
	select {}
}

// IsLeader returns whether this node is the current leader.
func (s *Server) IsLeader() bool {
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	return s.Role == types.Leader
}

// GetLeader returns the current known leader address.
// Leader returns its own address; follower/candidate learns via heartbeat,
// returns empty string if unknown.
func (s *Server) GetLeader() string {
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	if s.Role == types.Leader {
		return s.LocalID.Addr
	}
	return s.leaderAddr
}

// GetRole returns the current node role.
func (s *Server) GetRole() types.CMRole {
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	return s.Role
}

// MaybeStartSnap returns true if a snapshot should be triggered for a learner.
func (s *Server) MaybeStartSnap() bool {
	if s.Role != types.Learner || s.IsSnaping || len(s.Logs) == 0 {
		return false
	}
	return s.AppliedIndex-s.Logs[0].Index >= s.MaxIndexSpan
}
