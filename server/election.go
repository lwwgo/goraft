package server

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lwwgo/goraft/util"
)

// 投票请求
type RequestVote struct {
	Type MessageType
	// 发起投票请求节点的当前任期号
	Term        uint64
	CandidateID Peer
	// 发起投票节点在日志中的最后任期号
	LastTerm uint64
	// 发起投票节点在日志中的最后编号
	LastIndex uint64
}

// 投票响应
type ResponseVote struct {
	// 接收节点所在的任期
	Term uint64
	// true: 赞成; false: 反对
	VoteGranted bool
}

func (nd *Server) Addr() string {
	return nd.LocalID.Addr
}

func (nd *Server) VoteHandler(req RequestVote, resp *ResponseVote) error {
	if req.Type != MsgVote {
		log.Printf("do not support request message type, msgType:%s\n", req.Type.String())
		return errors.New("message type not supported")
	}
	log.Printf("receive vote request:%+v\n", req)

	nd.MuLock.Lock()
	defer nd.MuLock.Unlock()

	resp.VoteGranted = false
	LastTerm := nd.getLastLogTerm()
	lastIndex := nd.getLastLogIndex()
	// 接收到的任期 > 节点任期, 并且节点没有投过票或者在上一轮投票给了接收到的节点, 并且接收到的节点日志比本节点日志新
	// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号来定义谁的日志比较新
	// 如果两份日志最后条目的任期号不同，那么任期号大的日志更新
	// 如果两份日志最后条目的任期号相同，那么日志较长的那个更新。
	if req.Term > nd.Term && nd.Role != Learner &&
		(nd.VotedFor.Empty() || nd.VotedFor.Equal(req.CandidateID)) &&
		(req.LastTerm > LastTerm || (req.LastTerm == LastTerm && req.LastIndex >= lastIndex)) {
		log.Printf("receive vote request from %s, change state from %s to %s\n", req.CandidateID.Addr, nd.Role.String(), Follower.String())
		nd.Role = Follower
		nd.Term = req.Term
		nd.VotedFor = req.CandidateID
		nd.ElectionTimeStart = time.Now()
		resp.VoteGranted = true
	}

	resp.Term = nd.Term
	return nil
}

func (nd *Server) Elect() {
	var wg sync.WaitGroup
	winCount := int64(1)
	request := RequestVote{
		Type:        MsgVote,
		Term:        nd.Term,
		CandidateID: nd.LocalID,
		LastIndex:   nd.getLastLogIndex(),
		LastTerm:    nd.getLastLogTerm(),
	}
	for _, peer := range nd.Peers {
		// learner 不参与投票
		if nd.Role != Candidate || peer.Role == Learner {
			return
		}

		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			response := &ResponseVote{}
			err := util.RpcCallTimeout(peer.Addr, "Server.VoteHandler", request, response, 2*time.Second)
			if err != nil {
				log.Printf("rpc client send request failed, err:%s\n", err.Error())
			}
			if response.VoteGranted {
				atomic.AddInt64(&winCount, 1)
			} else {
				log.Printf("vote request failed from %s, voteGranted:%v\n", peer.Addr, response.VoteGranted)
			}
			// 选票超过集群中节点数量的一半, 则当选
			if nd.Role != Leader && int(winCount*2) > len(nd.Peers) {
				// 成为主节点
				nd.MuLock.Lock()
				nd.Role = Leader
				nd.MuLock.Unlock()
				log.Printf("server[%s] won the election, become to be leader, winCount:%d, sum:%d\n", nd.LocalID.Addr, winCount, len(nd.Peers)+1)

				// 通过心跳, 通知其他从节点结束本轮选举
				nd.SendHeartbeat()
				return
			}
		}(peer)
	}

	wg.Wait()
	if int(winCount*2) <= len(nd.Peers) {
		if nd.Role == Candidate {
			nd.MuLock.Lock()
			if nd.Role == Candidate {
				nd.VotedFor = Peer{}
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

// 周期性检查是否发起选举投票
func (nd *Server) RunElectionTimer() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		if nd.Role == Leader {
			continue
		}

		if time.Since(nd.ElectionTimeStart) >= nd.timeOutInternal() {
			// 选举超时, 成为候选节点, 首先增加任期号, 并投自己一票
			oldState := nd.Role
			nd.MuLock.Lock()
			nd.Role = Candidate
			nd.Term++
			nd.VotedFor = nd.LocalID
			nd.ElectionTimeStart = time.Now()
			nd.MuLock.Unlock()

			// 向其他节点发起选举请求
			log.Printf("change state from %s to %s, began to launch an election\n", oldState.String(), Candidate.String())
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

// 发送一次心跳
func (s *Server) SendHeartbeat() {
	// 只有主才会向其他节点发送心跳
	if s.Role != Leader {
		return
	}

	log.Printf("leader[%s] start to send heartbeat\n", s.LocalID.Addr)
	requestAppend := &RequestAppend{
		Type:            MsgHeartbeat,
		Term:            s.Term,
		LeaderID:        s.LocalID,
		PreLogIndex:     s.getLastLogIndex(),
		PreLogTerm:      s.getLastLogTerm(),
		LeaderCommitted: s.CommittedIndex,
	}
	for _, peer := range s.Peers {
		go func(peer Peer) {
			responseAppend := &ResponseAppend{}
			if err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second); err != nil {
				log.Printf("send heartbeat failed, from %s to %s\n", s.LocalID.Addr, peer.Addr)
			}
		}(peer)
	}
	log.Printf("leader[%s] send heartbeat end\n", s.LocalID.Addr)
}

// 周期性发送心跳
func (nd *Server) RunHeartbeatTimer() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		if nd.Role == Leader {
			// 向其他节点发起选举请求
			nd.SendHeartbeat()
		}
	}
}
