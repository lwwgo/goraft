package raft

import (
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lwwgo/goraft/snapshot"
	"github.com/lwwgo/goraft/types"
	"github.com/lwwgo/goraft/util"
)

func (s *Server) AppendEntryHandler(req *types.RequestAppend, resp *types.ResponseAppend) error {
	defer func() {
		if r := recover(); r != nil {
			buff := make([]byte, 1<<10)
			runtime.Stack(buff, false)
			log.Printf("%v %v\n", r, string(buff))
		}
	}()
	log.Printf("receive %s request from %s, local log length:%d, appendEntry length:%d, logs:%+v, req:%+v, local server logs:%+v\n",
		req.Type.String(), req.LeaderID.Addr, len(s.Logs), len(req.Entries), req.Entries, *req, s.Logs)
	// A candidate may receive an AppendEntries RPC from another server claiming
	// to be leader. If the leader's term >= candidate's term, the candidate
	// acknowledges the leader and returns to follower. If the term is smaller,
	// the candidate rejects and stays candidate.
	if req.Type == types.MsgHeartbeat && req.Term < s.Term {
		resp.Success = false
		resp.Term = s.Term
		return errors.New("term is smaller than local term, reject this heartbeat request")
	}

	if req.Type == types.MsgHeartbeat {
		s.MuLock.Lock()
		// Update term if leader's term is larger.
		if req.Term > s.Term {
			s.Term = req.Term
			s.VotedFor = types.Peer{}
		}
		if s.Role != types.Follower && s.Role != types.Learner {
			log.Printf("change state from %s to %s\n", s.Role.String(), types.Follower.String())
			s.Role = types.Follower
			s.VotedFor = req.LeaderID
		}
		// Record current leader address for upper-layer GetLeader().
		s.leaderAddr = req.LeaderID.Addr
		// Reset election timeout on heartbeat.
		s.ElectionTimeStart = time.Now()
		// Commit local logs to match leader's committed index.
		if req.LeaderCommitted > s.CommittedIndex {
			oldCommitIdx := s.CommittedIndex
			startIdx := s.getStartIndex()
			lastIdx := s.getLastLogIndex()
			s.CommittedIndex = min(lastIdx, req.LeaderCommitted)
			for i := uint64(1); i <= s.CommittedIndex-oldCommitIdx; i++ {
				index := oldCommitIdx + i
				if index >= startIdx {
					s.stateMachine.Apply(s.Logs[index-startIdx].Command.Op, s.Logs[index-startIdx].Command.Data)
					s.AppliedIndex = index
				}
			}
		}
		resp.Success = true
		resp.Term = s.Term
		s.MuLock.Unlock()
		log.Printf("handle heartbeat succ, response:%+v\n", resp)
		return nil
	}

	resp.Success = false
	if req.Type == types.MsgAppendLog {
		s.MuLock.Lock()
		defer s.MuLock.Unlock()

		// Update term and become follower if leader's term is larger.
		if req.Term > s.Term {
			s.Term = req.Term
			s.Role = types.Follower
			s.VotedFor = types.Peer{}
		}
		s.leaderAddr = req.LeaderID.Addr
		s.ElectionTimeStart = time.Now()

		// Log consistency check: follower's log must match leader's at PreLogIndex.
		var lastIdx, lastTerm uint64
		if n := len(s.Logs); n > 0 {
			lastIdx = s.Logs[n-1].Index
			lastTerm = s.Logs[n-1].Term
		}
		// PreLogIndex==0 means leader has no preceding log (empty state), accept directly.
		matched := req.PreLogIndex == 0 ||
			(req.PreLogIndex == lastIdx && req.PreLogTerm == lastTerm) ||
			(req.PreLogIndex == s.Snap.EndIndex && req.PreLogTerm == s.Snap.EndTerm)

		if !matched {
			log.Printf("preLogIndex or preLogTerm not match, local lastIdx:%d lastTerm:%d, req preLogIndex:%d preLogTerm:%d\n",
				lastIdx, lastTerm, req.PreLogIndex, req.PreLogTerm)
			return errors.New("preLogIndex or preLogTerm is not match")
		}

		// 1. Write to local memory.
		s.Logs = append(s.Logs, req.Entries...)
		// 2. Write to local WAL.
		for _, value := range req.Entries {
			logEntry := &types.LogEntry{Command: value.Command, Term: value.Term, Index: value.Index}
			s.WAL.Append(logEntry)
		}
		// 3. Commit will be checked on the next heartbeat.

		resp.Success = true
		log.Printf("append log entry succ, log:%+v\n", req.Entries)
		if s.MaybeStartSnap() {
			s.IsSnaping = true
			snapshotter := snapshot.New(s.Logs[0].Term, s.Logs[0].Index, s.Snap.WorkPath)
			log.Printf("start to make snapshot file:%s\n", snapshotter.GetPath())
			go func() {
				s.MuLock.Lock()
				snapMeta := types.SnapshotMetadata{Index: s.AppliedIndex, Term: s.Logs[s.AppliedIndex-s.Logs[0].Index].Term}
				data, err := s.stateMachine.Snapshot()
				if err != nil {
					s.logger.Error("state machine snapshot failed", "error", err)
					s.IsSnaping = false
					s.MuLock.Unlock()
					return
				}
				snap := &types.Snapshot{Data: data, Metadata: snapMeta}
				snapshotter.Save(snap)
				snapshotter.EndIndex = snapMeta.Index
				snapshotter.EndTerm = snapMeta.Term
				s.Snap = snapshotter
				s.Logs = s.Logs[snapshotter.EndIndex-snapshotter.StartIndex+1:]
				s.MuLock.Unlock()
				log.Printf("make snapshot file succ\n")
			}()
		}
	}

	return nil
}

func (s *Server) findConsistencyPoint(peer types.Peer) uint64 {
	for {
		preLogTerm, preLogIndex := s.peerPreTermAndIndex(peer)

		// Read NextIndex and log entry into local variables, release lock before RPC
		// to avoid blocking other followers' replication on RPC timeout.
		s.MuLock.Lock()
		logIndex := s.NextIndex[peer.Addr]
		start := s.getStartIndex()
		var entry types.LogEntry
		if logIndex >= start && int(logIndex-start) < len(s.Logs) {
			entry = s.Logs[logIndex-start]
		}
		s.MuLock.Unlock()

		requestAppend := &types.RequestAppend{
			Type:        types.MsgAppendLog,
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: preLogIndex,
			PreLogTerm:  preLogTerm,
			Entries:     []types.LogEntry{entry},
		}

		responseAppend := &types.ResponseAppend{}
		err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second)
		if err != nil {
			log.Printf("repair log failed from leader[%s] to follower[%s]\n", s.LocalID.Addr, peer.Addr)
			break
		}
		if responseAppend.Success {
			break
		}
		// Follower mismatch: decrement nextIndex and retry.
		s.MuLock.Lock()
		if requestAppend.PreLogIndex > 0 {
			s.NextIndex[peer.Addr] = requestAppend.PreLogIndex - 1
			s.MuLock.Unlock()
		} else {
			s.NextIndex[peer.Addr] = 0
			s.MuLock.Unlock()
			break
		}
	}
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	return s.NextIndex[peer.Addr]
}

func (s *Server) repairLog(peer types.Peer) bool {
	cPoint := s.findConsistencyPoint(peer)

	for {
		s.MuLock.Lock()
		logIndex := s.NextIndex[peer.Addr]
		if logIndex <= cPoint {
			logIndex = cPoint + 1
			s.NextIndex[peer.Addr] = logIndex
		}
		start := s.getStartIndex()
		if int(logIndex-start) >= len(s.Logs) {
			s.MuLock.Unlock()
			return true
		}
		entry := s.Logs[logIndex-start]
		var preLogTerm uint64
		if logIndex-1 >= start && int(logIndex-1-start) < len(s.Logs) {
			preLogTerm = s.Logs[logIndex-1-start].Term
		}
		s.MuLock.Unlock()

		requestAppend := &types.RequestAppend{
			Type:        types.MsgAppendLog,
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: logIndex - 1,
			PreLogTerm:  preLogTerm,
			Entries:     []types.LogEntry{entry},
		}

		responseAppend := &types.ResponseAppend{}
		err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second)
		if err != nil || !responseAppend.Success {
			log.Printf("repair log failed from leader[%s] to follower[%s]\n", s.LocalID.Addr, peer.Addr)
			return false
		}
		s.MuLock.Lock()
		s.NextIndex[peer.Addr] = logIndex + 1
		s.MuLock.Unlock()
		log.Printf("follower[%s] repair log succ, index:%d, term:%d\n", peer.Addr, logIndex, entry.Term)
	}
}

// WriteLog replicates a log entry to all followers (leader only).
func (s *Server) WriteLog(command types.CommandEntry) (types.LogEntry, error) {
	// Serialize all writes to prevent NextIndex/log index corruption.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// 1. Write to local memory (compute correct Index under lock, considering snapshot truncation).
	s.MuLock.Lock()
	var nextIndex uint64
	if len(s.Logs) > 0 {
		nextIndex = s.Logs[len(s.Logs)-1].Index + 1
	} else if s.Snap.EndIndex > 0 {
		nextIndex = s.Snap.EndIndex + 1
	} else {
		nextIndex = 1 // first log starts at index 1 (Raft convention; 0 = empty)
	}
	logEntry := types.LogEntry{
		Command: command,
		Term:    s.Term,
		Index:   nextIndex,
	}
	s.Logs = append(s.Logs, logEntry)
	currentIndex := int64(len(s.Logs) - 1)
	s.MuLock.Unlock()
	// 2. Write to local WAL.
	s.WAL.Append(&logEntry)

	// Local write already counts as 1 success (atomic counter to avoid races).
	var succ int64 = 1
	log.Printf("leader write log, logEntry:%+v\n", logEntry)

	// 3. Concurrently replicate to other nodes.
	var wg sync.WaitGroup
	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer types.Peer) {
			defer wg.Done()
			preLogTerm, preLogIndex := s.peerPreTermAndIndex(peer)
			requestAppend := types.RequestAppend{
				Type:            types.MsgAppendLog,
				Term:            s.Term,
				LeaderID:        s.LocalID,
				PreLogIndex:     preLogIndex,
				PreLogTerm:      preLogTerm,
				Entries:         []types.LogEntry{logEntry},
				LeaderCommitted: s.CommittedIndex,
			}

			defer func() {
				if r := recover(); r != nil {
					buff := make([]byte, 1<<10)
					runtime.Stack(buff, false)
					log.Printf("recover info: %v %v\n", r, string(buff))
				}
			}()

			responseAppend := &types.ResponseAppend{}
			log.Printf("start to write log, origin:%s, dest:%s, req:%+v\n", s.LocalID.Addr, peer.Addr, requestAppend)
			err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 1*time.Second)
			if err != nil {
				log.Printf("write log replica failed to follower[%s], err:%s\n", peer.Addr, err.Error())
				return
			}
			if responseAppend.Success {
				if peer.Role != types.Learner {
					atomic.AddInt64(&succ, 1)
				}
				s.MuLock.Lock()
				// After success, NextIndex should be current log Index + 1.
				if logEntry.Index+1 > s.NextIndex[peer.Addr] {
					s.NextIndex[peer.Addr] = logEntry.Index + 1
				}
				s.MuLock.Unlock()
				log.Printf("write log succ on dest:%s\n", peer.Addr)
				return
			}
			// Follower rejected: decrement nextIndex and trigger log repair.
			s.MuLock.Lock()
			if requestAppend.PreLogIndex > 0 {
				s.NextIndex[peer.Addr] = requestAppend.PreLogIndex - 1
			} else {
				s.NextIndex[peer.Addr] = 0
			}
			s.MuLock.Unlock()
			repairSucc := s.repairLog(peer)
			if repairSucc && peer.Role != types.Learner {
				atomic.AddInt64(&succ, 1)
			}
		}(peer)
	}
	wg.Wait()

	// 4. If majority succeeded, commit; otherwise roll back local memory.
	if int(succ*2) > len(s.Peers) {
		s.MuLock.Lock()
		s.incCommitedIndex()
		s.MuLock.Unlock()
		log.Printf("write %d log-replicas succ, it is committed\n", succ)
		return logEntry, nil
	}
	s.MuLock.Lock()
	s.Logs = append(s.Logs[:currentIndex], s.Logs[currentIndex+1:]...)
	s.MuLock.Unlock()
	return types.LogEntry{}, errors.New("write log-replicas failed")
}

func (s *Server) incCommitedIndex() {
	// Log indices are 1-based, so the first committed entry advances
	// CommittedIndex from 0 to 1. No special-case needed.
	s.CommittedIndex++
}

func (s *Server) peerPreTermAndIndex(peer types.Peer) (uint64, uint64) {
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	next := s.NextIndex[peer.Addr]
	if next <= 1 {
		// next=1 means we are about to send the first log entry
		// (Index=1); there is no preceding entry to match.
		return 0, 0
	}
	start := s.getStartIndex()
	preIndex := next - 1
	if preIndex < start {
		// Preceding entry is before our current log window: it was
		// truncated by a snapshot (or the log is empty).
		return s.Snap.EndTerm, s.Snap.EndIndex
	}
	return s.Logs[preIndex-start].Term, preIndex
}

// Do submits a consensus command (leader only).
func (s *Server) Do(command types.CommandEntry) error {
	logEntry, err := s.WriteLog(command)
	if err != nil {
		return fmt.Errorf("write log replica failed: %w", err)
	}
	err = s.stateMachine.Apply(logEntry.Command.Op, logEntry.Command.Data)
	if err != nil {
		log.Printf("apply log to business state machine failed, log:%+v, err:%v\n", logEntry, err)
		return fmt.Errorf("apply log to business state machine failed: %w", err)
	}
	return nil
}
