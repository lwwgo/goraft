package server

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/lwwgo/goraft/util"
)

// 复制日志rpc请求
type RequestAppend struct {
	Type            MessageType
	Term            uint64
	LeaderID        Peer
	PreLogIndex     uint64
	PreLogTerm      uint64
	Entries         []LogEntry
	LeaderCommitted uint64
}

// 复制日志rpc响应
type ResponseAppend struct {
	Term    uint64
	Success bool
}

func (s *Server) AppendEntryHandler(req *RequestAppend, resp *ResponseAppend) error {
	log.Printf("receive %s request from %s, local log length:%d, appendEntry length:%d, logs:%+v, req:%+v, local server logs:%+v\n",
		req.Type.String(), req.LeaderID.Addr, len(s.Logs), len(req.Entries), req.Entries, *req, s.Logs)
	// 接收主节点广播选举结果, 本节点主动变为从
	// 在等待投票期间，candidate 可能会收到另一个声称自己是 leader 的服务器节点发来的 AppendEntries RPC
	// 如果这个 leader 的任期号（包含在RPC中）不小于 candidate 当前的任期号，那么 candidate 会承认该 leader 的合法地位并回到 follower 状态
	// 如果 RPC 中的任期号比自己的小，那么 candidate 就会拒绝这次的 RPC 并且继续保持 candidate 状态
	if req.Type == MsgHeartbeat && req.Term >= s.Term {
		s.MuLock.Lock()
		if s.State != Follower {
			log.Printf("change state from %s to %s\n", s.State.String(), Follower.String())
			s.State = Follower
			s.VotedFor = req.LeaderID
		}
		// 正常接收心跳, 则重置选举超时时间
		s.ElectionTimeStart = time.Now()
		// 提交本地日志, 与leader保持提交一致
		if req.LeaderCommitted > s.CommittedIndex {
			oldCommit := s.CommittedIndex
			s.CommittedIndex = util.Min(uint64(len(s.Logs)-1), req.LeaderCommitted)
			for i := uint64(1); i <= s.CommittedIndex-oldCommit; i++ {
				index := oldCommit + i
				s.bizApplyFunc(s.Logs[index])
				s.AppliedIndex = index
			}
		}
		resp.Success = true
		resp.Term = s.Term
		s.MuLock.Unlock()
		log.Printf("handle heartbeat succ, response:%+v\n", resp)
		return nil
	}

	resp.Success = false
	if req.Type == MsgAppendLog {
		// 同步日志, 一致性检查保证跟随者日志和领导者日志相同(已提交日志)
		if (req.PreLogIndex == 0 && req.LeaderCommitted == 0) ||
			(req.PreLogIndex == s.Logs[len(s.Logs)-1].Index && req.PreLogTerm == s.Logs[req.PreLogIndex].Term) {
			// 1. 写本地内存
			s.Logs = append(s.Logs, req.Entries...)
			// 2. 写本地wal
			for _, value := range req.Entries {
				logEntry := &LogEntry{Command: value.Command, Term: value.Term, Index: value.Index}
				s.Persist.Append(logEntry)
			}
			// 3. 在下一次心跳中检查leader committedIndex, 在心跳中提交历史上leader已提交的日志

			resp.Success = true
			log.Printf("append log entry succ, log:%+v\n", req.Entries)
			if s.MaybeStartSnap() {
				s.IsSnaping = true
				snapshot := NewSnap(s.Logs[0].Term, s.Logs[0].Index, s.Snap.WorkPath)
				s.Snap = snapshot
				log.Printf("start to make snapshot file:%s\n", snapshot.GetPath())
				go snapshot.Save(s.getSnapshot())
			}
		} else {
			// 清理不一致日志
			s.Logs = s.Logs[:len(s.Logs)-1]
		}
	}

	return nil
}

func (s *Server) findConsistencyPoint(peer Peer) uint64 {
	preLogTerm, preLogIndex := s.peerPreTermAndIndex(peer)
	logIndex := s.NextIndex[peer.Addr]
	requestAppend := &RequestAppend{
		Term:        s.Term,
		LeaderID:    s.LocalID,
		PreLogIndex: preLogIndex,
		PreLogTerm:  preLogTerm,
		Entries:     []LogEntry{s.Logs[logIndex]},
	}

	responseAppend := &ResponseAppend{}
	err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second)
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

	for logIndex := cPoint + 1; logIndex < uint64(len(s.Logs)); logIndex++ {
		logIndex := s.NextIndex[peer.Addr]
		requestAppend := &RequestAppend{
			Type:        MsgAppendLog,
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: logIndex - 1,
			PreLogTerm:  s.Logs[logIndex-1].Term,
			Entries:     []LogEntry{s.Logs[logIndex]},
		}

		responseAppend := &ResponseAppend{}
		err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second)
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
func (s *Server) WriteLog(command CommandEtnry) (LogEntry, error) {
	logEntry := LogEntry{
		Command: command,
		Term:    s.Term,
		Index:   uint64(len(s.Logs)),
	}
	// 1. 写本节点内存
	s.MuLock.Lock()
	s.Logs = append(s.Logs, logEntry)
	currentIndex := int64(len(s.Logs) - 1)
	s.MuLock.Unlock()
	// 2. 写本地wal
	s.Persist.Append(&logEntry)

	// 本节点已经写入, 成功数量起始值应为 1
	succ := 1
	log.Printf("leader write log, logEntry:%+v\n", logEntry)

	// 3. 并发写其他节点
	var wg sync.WaitGroup
	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			log.Printf("server nextIndex: %+v\n", s.NextIndex)
			preLogTerm, preLogIndex := s.peerPreTermAndIndex(peer)
			requestAppend := RequestAppend{
				Type:            MsgAppendLog,
				Term:            s.Term,
				LeaderID:        s.LocalID,
				PreLogIndex:     preLogIndex,
				PreLogTerm:      preLogTerm,
				Entries:         []LogEntry{logEntry},
				LeaderCommitted: s.CommittedIndex,
			}

			defer func() {
				if r := recover(); r != nil {
					buff := make([]byte, 1<<10)
					runtime.Stack(buff, false)
					log.Printf("recover info: %v %v\n", r, string(buff))
				}
			}()

			responseAppend := &ResponseAppend{}
			log.Printf("start to write log, origin:%s, dest:%s, req:%+v\n", s.LocalID.Addr, peer.Addr, requestAppend)
			err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 1*time.Second)
			if err != nil {
				log.Printf("write log replica failed to follower[%s], err:%s\n", peer.Addr, err.Error())
			} else if responseAppend.Success {
				if peer.State != Learner {
					succ++
				}
				s.NextIndex[peer.Addr]++
				log.Printf("write log succ on dest:%s\n", peer.Addr)
			} else if !responseAppend.Success {
				if requestAppend.PreLogIndex > 0 {
					s.NextIndex[peer.Addr] = requestAppend.PreLogIndex - 1
				} else {
					s.NextIndex[peer.Addr] = 0
				}
				// follower 追 leader 的日志
				repairSucc := s.repairLog(peer)
				if repairSucc {
					succ++
				}
			}
		}(peer)
	}
	wg.Wait()

	// 4. 复制日志成功, leader 标记日志为 [已提交]; 否则, 回滚本地内存
	if succ*2 > len(s.Peers) {
		s.MuLock.Lock()
		s.incCommitedIndex()
		s.MuLock.Unlock()
		log.Printf("write %d log-replicas succ, it is committed\n", succ)
		return logEntry, nil
	}
	s.MuLock.Lock()
	s.Logs = append(s.Logs[:currentIndex], s.Logs[currentIndex+1:]...)
	s.MuLock.Unlock()
	return LogEntry{}, errors.New("write log-replicas failed")
}

func (s *Server) incCommitedIndex() {
	if s.CommittedIndex == 0 && len(s.Logs) == 1 {
		s.CommittedIndex = 0
	} else {
		s.CommittedIndex++
	}
}

func (s *Server) peerPreTermAndIndex(peer Peer) (uint64, uint64) {
	next := s.NextIndex[peer.Addr]
	if next == 0 {
		return 0, 0
	}
	return s.Logs[next-1].Term, next - 1
}

func (s *Server) Do(command CommandEtnry) error {
	logEntry, err := s.WriteLog(command)
	if err != nil {
		return errors.New("write log replica failed\n")
	}
	err = s.bizApplyFunc(logEntry)
	if err != nil {
		log.Printf("apply log to bussines state machine failed, log:%+v\n", logEntry)
		return errors.New("apply log to bussines state machine failed")
	}
	return nil
}
