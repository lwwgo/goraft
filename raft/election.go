package raft

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lwwgo/goraft/types"
	"github.com/lwwgo/goraft/util"
)

func (nd *Server) Addr() string {
	return nd.LocalID.Addr
}

func (nd *Server) VoteHandler(req types.RequestVote, resp *types.ResponseVote) error {
	if nd.Role == types.Learner {
		log.Printf("learner do not support vote request\n")
		return nil
	}

	if req.Type != types.MsgVote {
		log.Printf("do not support request message type, msgType:%s\n", req.Type.String())
		return errors.New("message type not supported")
	}

	log.Printf("receive vote request:%+v\n", req)
	nd.MuLock.Lock()
	defer nd.MuLock.Unlock()

	resp.VoteGranted = false
	LastTerm := nd.getLastLogTerm()
	lastIndex := nd.getLastLogIndex()
	// Voting rules (all three must hold):
	// 1. req.Term >= local term (standard Raft: same term is OK if not yet voted)
	// 2. not yet voted, or voted for this candidate in this term
	// 3. candidate's log is at least as new as local log
	if req.Term >= nd.Term &&
		(nd.VotedFor.Empty() || nd.VotedFor.Equal(req.CandidateID)) &&
		(req.LastTerm > LastTerm || (req.LastTerm == LastTerm && req.LastIndex >= lastIndex)) {
		if req.Term > nd.Term {
			nd.Term = req.Term
			nd.VotedFor = types.Peer{}
		}
		log.Printf("receive vote request from %s, change state from %s to %s\n", req.CandidateID.Addr, nd.Role.String(), types.Follower.String())
		nd.Role = types.Follower
		nd.VotedFor = req.CandidateID
		nd.ElectionTimeStart = time.Now()
		resp.VoteGranted = true
	}

	resp.Term = nd.Term
	return nil
}

func (nd *Server) Elect() {
	if nd.Role != types.Candidate {
		return
	}

	var wg sync.WaitGroup
	winCount := int64(1)
	request := types.RequestVote{
		Type:        types.MsgVote,
		Term:        nd.Term,
		CandidateID: nd.LocalID,
		LastIndex:   nd.getLastLogIndex(),
		LastTerm:    nd.getLastLogTerm(),
	}
	for _, peer := range nd.Peers {
		// Learners do not participate in voting.
		if peer.Role == types.Learner {
			continue
		}

		wg.Add(1)
		go func(peer types.Peer) {
			defer wg.Done()
			response := &types.ResponseVote{}
			err := util.RpcCallTimeout(peer.Addr, "Server.VoteHandler", request, response, 2*time.Second)
			if err != nil {
				log.Printf("rpc client send request failed, err:%s\n", err.Error())
			}
			if response.VoteGranted {
				atomic.AddInt64(&winCount, 1)
			} else {
				log.Printf("vote request failed from %s, voteGranted:%v\n", peer.Addr, response.VoteGranted)
			}
			// Win if votes exceed half of the cluster.
			if nd.Role != types.Leader && int(winCount*2) > len(nd.Peers) {
				nd.MuLock.Lock()
				if nd.Role == types.Leader {
					nd.MuLock.Unlock()
					return
				}
				nd.Role = types.Leader
				nd.LocalID.Role = types.Leader
				nd.leaderAddr = nd.LocalID.Addr
				// Reset all followers' NextIndex to lastLogIndex + 1,
				// then probe backwards to find consistency point on demand.
				lastIdx := nd.getLastLogIndex()
				for _, p := range nd.Peers {
					nd.NextIndex[p.Addr] = lastIdx + 1
				}
				nd.MuLock.Unlock()
				log.Printf("server[%s] won the election, become to be leader, winCount:%d, sum:%d\n", nd.LocalID.Addr, winCount, len(nd.Peers)+1)

				// Notify followers via heartbeat to end this election round.
				nd.SendHeartbeat()
				return
			}
		}(peer)
	}

	wg.Wait()
	if int(winCount*2) <= len(nd.Peers) {
		if nd.Role == types.Candidate {
			nd.MuLock.Lock()
			if nd.Role == types.Candidate {
				nd.VotedFor = types.Peer{}
			}
			nd.MuLock.Unlock()
		}
		log.Printf("server[%s] lost the election, winCount:%d, sum:%d\n", nd.LocalID.Addr, winCount, len(nd.Peers)+1)
	}
}

func (nd *Server) timeOutInternal() time.Duration {
	left, delta := int((1-nd.TimeOutRandomFactor)*100), int(nd.TimeOutRandomFactor*100*2)
	randDelta := rand.Intn(delta)
	return time.Duration(float64(left+randDelta) / 100 * float64(nd.TimeOut))
}

// RunElectionTimer periodically checks whether an election should start.
func (nd *Server) RunElectionTimer() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		if nd.Role == types.Leader {
			continue
		}

		if time.Since(nd.ElectionTimeStart) >= nd.timeOutInternal() {
			oldState := nd.Role
			nd.MuLock.Lock()
			nd.Role = types.Candidate
			nd.Term++
			nd.VotedFor = nd.LocalID
			nd.ElectionTimeStart = time.Now()
			nd.MuLock.Unlock()

			log.Printf("change state from %s to %s, began to launch an election\n", oldState.String(), types.Candidate.String())
			nd.Elect()
			log.Printf("the election server[%s] initiated is over\n", nd.LocalID.Addr)
		}
	}
}

func (s *Server) getStartIndex() uint64 {
	if len(s.Logs) == 0 {
		return s.CommittedIndex
	}
	return s.Logs[0].Index
}

func (s *Server) getLastLogIndex() uint64 {
	if len(s.Logs) == 0 {
		return s.CommittedIndex
	}
	return s.Logs[len(s.Logs)-1].Index
}

func (s *Server) getLastLogTerm() uint64 {
	if len(s.Logs) == 0 {
		return s.Term
	}
	return s.Logs[len(s.Logs)-1].Term
}

// SendHeartbeat sends one round of heartbeats to all followers (leader only).
func (s *Server) SendHeartbeat() {
	if s.Role != types.Leader {
		return
	}

	log.Printf("leader[%s] start to send heartbeat\n", s.LocalID.Addr)
	requestAppend := &types.RequestAppend{
		Type:            types.MsgHeartbeat,
		Term:            s.Term,
		LeaderID:        s.LocalID,
		PreLogIndex:     s.getLastLogIndex(),
		PreLogTerm:      s.getLastLogTerm(),
		LeaderCommitted: s.CommittedIndex,
	}
	for _, peer := range s.Peers {
		go func(peer types.Peer) {
			responseAppend := &types.ResponseAppend{}
			if err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second); err != nil {
				log.Printf("send heartbeat failed, from %s to %s\n", s.LocalID.Addr, peer.Addr)
			}
		}(peer)
	}
	log.Printf("leader[%s] send heartbeat end\n", s.LocalID.Addr)
}

// RunHeartbeatTimer periodically sends heartbeats (leader only).
// The interval is configured via HeartbeatInterval (defaults to
// ElectionTimeout / 5), which must be smaller than the election timeout
// to prevent spurious leader elections.
func (nd *Server) RunHeartbeatTimer() {
	interval := nd.HeartbeatInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		<-ticker.C
		if nd.Role == types.Leader {
			nd.SendHeartbeat()
		}
	}
}
