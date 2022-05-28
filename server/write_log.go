package server

import (
	"log"
	"raft/util"
	"runtime"
	"sync"
	"time"
)

// 复制日志rpc请求
type RequestAppend struct {
	Term         int
	LeaderID     Peer
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

// 复制日志rpc响应
type ResponseAppend struct {
	Term    int
	Success bool
}

func (s *Server) AppendEntryHandler(req *RequestAppend, resp *ResponseAppend) error {
	log.Printf("Server[%s] receive append log entry request from %s, local log lenght:%d, appendEntry lenght:%d, logs:%+v, req:%+v, local server logs:%+v\n",
		s.LocalID.Addr, req.LeaderID.Addr, len(s.Logs), len(req.Entries), req.Entries, *req, s.Logs)
	// 接收主节点广播选举结果, 本节点主动变为从
	// 在等待投票期间，candidate 可能会收到另一个声称自己是 leader 的服务器节点发来的 AppendEntries RPC
	// 如果这个 leader 的任期号（包含在RPC中）不小于 candidate 当前的任期号，那么 candidate 会承认该 leader 的合法地位并回到 follower 状态
	// 如果 RPC 中的任期号比自己的小，那么 candidate 就会拒绝这次的 RPC 并且继续保持 candidate 状态
	if len(req.Entries) == 0 && req.Term >= s.Term {
		s.MuLock.Lock()
		if s.State != Follower {
			log.Printf("receive append empty log-entry from leader[%s], Server[%s] change state from %s to %s\n",
				req.LeaderID, s.LocalID, s.State.String(), Follower.String())
			s.State = Follower
			s.VotedFor = Peer{}
		}
		// 正常接收心跳, 则重置选举超时时间
		s.ElectionTimeStart = time.Now()
		s.MuLock.Unlock()
		return nil
	}

	resp.Success = false
	// 同步日志, 一致性检查保证跟随者日志和领导者日志相同(已提交日志)
	if req.PreLogIndex == len(s.Logs)-1 && req.PreLogTerm == s.Logs[req.PreLogIndex].Term {
		s.Logs = append(s.Logs, req.Entries...)
		resp.Success = true
		log.Printf("append log entry succ, logs:%+v\n", s.Logs)
	} else {
		// 清理不一致日志
		s.Logs = s.Logs[:len(s.Logs)-1]
	}
	s.CommitIndex = len(s.Logs) - 1
	return nil
}

func (s *Server) findConsistencyPoint(peer Peer) int {
	logIndex := s.NextIndex[peer.Addr]
	requestAppend := &RequestAppend{
		Term:        s.Term,
		LeaderID:    s.LocalID,
		PreLogIndex: logIndex - 1,
		PreLogTerm:  s.Logs[logIndex-1].Term,
		Entries:     []LogEntry{s.Logs[logIndex]},
	}

	responseAppend := &ResponseAppend{}
	err := s.RpcClients[peer.Addr].Call("Server.AppendEntryHandler", requestAppend, responseAppend)
	if err != nil {
		log.Printf("repair log failed from leader[%s] to follower[%s]\n", s.LocalID.Addr, peer.Addr)
	} else if !responseAppend.Success {
		s.NextIndex[peer.Addr] = requestAppend.PreLogIndex
		log.Printf("follower[%s] does not match log preIndex:%d, preTerm:%d\n", peer.Addr, logIndex-1, s.Logs[logIndex-1].Term)
		s.findConsistencyPoint(peer)
	}
	return s.NextIndex[peer.Addr]
}

func (s *Server) repairLog(peer Peer) bool {
	cPoint := s.findConsistencyPoint(peer)

	for logIndex := cPoint + 1; logIndex < len(s.Logs); logIndex++ {
		logIndex := s.NextIndex[peer.Addr]
		requestAppend := &RequestAppend{
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: logIndex - 1,
			PreLogTerm:  s.Logs[logIndex-1].Term,
			Entries:     []LogEntry{s.Logs[logIndex]},
		}

		responseAppend := &ResponseAppend{}
		err := s.RpcClients[peer.Addr].Call("Server.AppendEntryHandler", requestAppend, responseAppend)
		if err != nil || !responseAppend.Success {
			log.Printf("repair log failed from leader[%s] to follower[%s]\n", s.LocalID.Addr, peer.Addr)
			return false
		}
		s.NextIndex[peer.Addr] = logIndex + 1
		log.Printf("follower[%s] repair log succ, index:%d, term:%d\n", peer.Addr, logIndex, s.Logs[logIndex].Term)
	}

	return true
}

// leader 向 follower 复制日志
func (s *Server) WriteLog(command CommandEtnry) (*LogEntry, bool) {
	logEntry := LogEntry{
		Command: command,
		Term:    s.Term,
	}
	// 写本节点
	s.MuLock.Lock()
	s.Logs = append(s.Logs, logEntry)
	s.MuLock.Unlock()
	// 本节点已经写入, 成功数量起始值应为 1
	succ := 1
	log.Printf("leader write log, logEntry:%+v\n", logEntry)

	// 并发写其他节点
	var wg sync.WaitGroup
	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			log.Printf("server nextIndex: %+v\n", s.NextIndex)
			preLogIndex, preLogTerm := s.NextIndex[peer.Addr]-1, 0
			if preLogIndex >= 0 {
				preLogTerm = s.Logs[preLogIndex].Term
			}
			requestAppend := RequestAppend{
				Term:         s.Term,
				LeaderID:     s.LocalID,
				PreLogIndex:  preLogIndex,
				PreLogTerm:   preLogTerm,
				Entries:      []LogEntry{logEntry},
				LeaderCommit: s.CommitIndex,
			}

			responseAppend := &ResponseAppend{}
			defer func() {
				if r := recover(); r != nil {
					buff := make([]byte, 1<<10)
					runtime.Stack(buff, false)
					log.Printf("recover info: %v %v\n", r, string(buff))
				}
			}()
			log.Printf("start to write log, origin:%s, dest:%s, requestAppend:%+v\n", s.LocalID.Addr, peer.Addr, requestAppend)
			err := util.RpcCallTimeout(s.RpcClients[peer.Addr], "Server.AppendEntryHandler", requestAppend, responseAppend, 1*time.Second)
			if err != nil {
				log.Printf("write log failed from leader[%s] to follower[%s], err:%s\n", s.LocalID.Addr, peer.Addr, err.Error())
			} else if responseAppend.Success {
				succ++
				s.NextIndex[peer.Addr]++
				log.Printf("write log succ on dest:%s\n", peer.Addr)
			} else if !responseAppend.Success {
				s.NextIndex[peer.Addr] = requestAppend.PreLogIndex - 1
				// follower 追 leader 的日志
				repairSucc := s.repairLog(peer)
				if repairSucc {
					succ++
				}
			}
		}(peer)
	}
	wg.Wait()

	// leader 标记日志为 [已提交]
	if succ*2 > len(s.Peers)+1 {
		s.MuLock.Lock()
		s.CommitIndex++
		s.MuLock.Unlock()
		log.Printf("write %d replica log succ\n", succ)
		return &logEntry, true
	}
	return nil, false
}

func (s *Server) ApplyLog(logEntry *LogEntry) bool {
	return true
}
