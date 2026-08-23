package server

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
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
	defer func() {
		if r := recover(); r != nil {
			buff := make([]byte, 1<<10)
			runtime.Stack(buff, false)
			log.Printf("%v %v\n", r, string(buff))
		}
	}()
	log.Printf("receive %s request from %s, local log length:%d, appendEntry length:%d, logs:%+v, req:%+v, local server logs:%+v\n",
		req.Type.String(), req.LeaderID.Addr, len(s.Logs), len(req.Entries), req.Entries, *req, s.Logs)
	// 接收主节点广播选举结果, 本节点主动变为从
	// 在等待投票期间，candidate 可能会收到另一个声称自己是 leader 的服务器节点发来的 AppendEntries RPC
	// 如果这个 leader 的任期号（包含在RPC中）不小于 candidate 当前的任期号，那么 candidate 会承认该 leader 的合法地位并回到 follower 状态
	// 如果 RPC 中的任期号比自己的小，那么 candidate 就会拒绝这次的 RPC 并且继续保持 candidate 状态
	if req.Type == MsgHeartbeat && req.Term < s.Term {
		resp.Success = false
		resp.Term = s.Term
		return errors.New("term is smaller than local term, reject this heartbeat request")
	}

	if req.Type == MsgHeartbeat {
		s.MuLock.Lock()
		if s.Role != Follower && s.Role != Learner {
			log.Printf("change state from %s to %s\n", s.Role.String(), Follower.String())
			s.Role = Follower
			s.VotedFor = req.LeaderID
		}
		// 正常接收心跳, 则重置选举超时时间
		s.ElectionTimeStart = time.Now()
		// 提交本地日志, 与leader保持提交一致
		if req.LeaderCommitted > s.CommittedIndex {
			oldCommitIdx := s.CommittedIndex
			startIdx := s.getStartIndex()
			lastIdx := s.getLastLogIndex()
			s.CommittedIndex = min(lastIdx, req.LeaderCommitted)
			for i := uint64(1); i <= s.CommittedIndex-oldCommitIdx; i++ {
				index := oldCommitIdx + i
				if index >= startIdx {
					s.bizApplyFunc(s.Logs[index-startIdx])
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
	if req.Type == MsgAppendLog {
		// 同步日志, 一致性检查保证跟随者日志和领导者日志相同(已提交日志)
		var lastIdx, lastTerm uint64
		if n := len(s.Logs); n > 0 {
			lastIdx = s.Logs[n-1].Index
			lastTerm = s.Logs[n-1].Term
		}
		matched := (req.PreLogIndex == 0 && req.LeaderCommitted == 0) ||
			(req.PreLogIndex == lastIdx && req.PreLogTerm == lastTerm) ||
			(req.PreLogIndex == s.Snap.EndIndex && req.PreLogTerm == s.Snap.EndTerm)

		if !matched {
			// 清理不一致日志
			s.Logs = s.Logs[:len(s.Logs)-1]
			return errors.New("preLogIndex or preLogTerm is not match")
		}

		// 1. 写本地内存
		s.Logs = append(s.Logs, req.Entries...)
		// 2. 写本地wal
		for _, value := range req.Entries {
			logEntry := &LogEntry{Command: value.Command, Term: value.Term, Index: value.Index}
			s.WAL.Append(logEntry)
		}
		// 3. 在下一次心跳中检查leader committedIndex, 在心跳中提交历史上leader已提交的日志

		resp.Success = true
		log.Printf("append log entry succ, log:%+v\n", req.Entries)
		if s.MaybeStartSnap() {
			s.IsSnaping = true
			snapshot := NewSnap(s.Logs[0].Term, s.Logs[0].Index, s.Snap.WorkPath)
			log.Printf("start to make snapshot file:%s\n", snapshot.GetPath())
			// 异步构建snapshot, 并清理内存中已Apply的日志
			go func() {
				s.MuLock.Lock()
				snapMeta := SnapshotMetadata{Index: s.AppliedIndex, Term: s.Logs[s.AppliedIndex-s.Logs[0].Index].Term}
				snap := s.getSnapshot()
				snap.Metadata = snapMeta
				snapshot.Save(snap)
				snapshot.EndIndex = snapMeta.Index
				snapshot.EndTerm = snapMeta.Term
				s.Snap = snapshot
				s.Logs = s.Logs[snapshot.EndIndex-snapshot.StartIndex+1:]
				s.MuLock.Unlock()
				log.Printf("make snapshot file succ\n")
			}()
		}
	}

	return nil
}

func (s *Server) findConsistencyPoint(peer Peer) uint64 {
	for {
		// peerPreTermAndIndex 内部已加锁, 返回 prevLog 的 term/index
		preLogTerm, preLogIndex := s.peerPreTermAndIndex(peer)

		// 读 NextIndex 和对应日志条目到局部变量, 释放锁后再发 RPC,
		// 避免 RPC 超时阻塞其他 follower 的复制
		s.MuLock.Lock()
		logIndex := s.NextIndex[peer.Addr]
		start := s.getStartIndex()
		var entry LogEntry
		if logIndex >= start && int(logIndex-start) < len(s.Logs) {
			entry = s.Logs[logIndex-start]
		}
		s.MuLock.Unlock()

		requestAppend := &RequestAppend{
			Type:        MsgAppendLog,
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: preLogIndex,
			PreLogTerm:  preLogTerm,
			Entries:     []LogEntry{entry},
		}

		responseAppend := &ResponseAppend{}
		err := util.RpcCallTimeout(peer.Addr, "Server.AppendEntryHandler", requestAppend, responseAppend, 2*time.Second)
		if err != nil {
			log.Printf("repair log failed from leader[%s] to follower[%s]\n", s.LocalID.Addr, peer.Addr)
			break
		}
		if responseAppend.Success {
			break
		}
		// follower 不匹配, nextIndex 退一格继续探测
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

func (s *Server) repairLog(peer Peer) bool {
	cPoint := s.findConsistencyPoint(peer)

	for {
		// 加锁读出本次要灌的 logIndex 和日志条目, 释放锁后再发 RPC
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

		requestAppend := &RequestAppend{
			Type:        MsgAppendLog,
			Term:        s.Term,
			LeaderID:    s.LocalID,
			PreLogIndex: logIndex - 1,
			PreLogTerm:  preLogTerm,
			Entries:     []LogEntry{entry},
		}

		responseAppend := &ResponseAppend{}
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
	s.WAL.Append(&logEntry)

	// 本节点已经写入, 成功数量起始值应为 1 (并发自增, 用原子计数避免数据竞争)
	var succ int64 = 1
	log.Printf("leader write log, logEntry:%+v\n", logEntry)

	// 3. 并发写其他节点
	var wg sync.WaitGroup
	for _, peer := range s.Peers {
		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			// 先加锁读出 prevLog 信息, 释放锁后再发 RPC,
			// 避免 RPC 超时阻塞其他 follower 的复制
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
				return
			}
			if responseAppend.Success {
				if peer.Role != Learner {
					atomic.AddInt64(&succ, 1)
				}
				s.MuLock.Lock()
				s.NextIndex[peer.Addr]++
				s.MuLock.Unlock()
				log.Printf("write log succ on dest:%s\n", peer.Addr)
				return
			}
			// follower 拒绝, 退回 nextIndex 并触发日志修复
			s.MuLock.Lock()
			if requestAppend.PreLogIndex > 0 {
				s.NextIndex[peer.Addr] = requestAppend.PreLogIndex - 1
			} else {
				s.NextIndex[peer.Addr] = 0
			}
			s.MuLock.Unlock()
			// follower 追 leader 的日志
			repairSucc := s.repairLog(peer)
			if repairSucc && peer.Role != Learner {
				atomic.AddInt64(&succ, 1)
			}
		}(peer)
	}
	wg.Wait()

	// 4. 复制日志成功, leader 标记日志为 [已提交]; 否则, 回滚本地内存
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
	s.MuLock.Lock()
	defer s.MuLock.Unlock()
	next := s.NextIndex[peer.Addr]
	if next == 0 {
		return 0, 0
	}
	start := s.getStartIndex()
	if next <= start {
		return s.Logs[0].Term, s.Logs[0].Index
	}
	if int(next-1-start) >= len(s.Logs) {
		// nextIndex 超出当前内存日志范围, 退回到最后一条日志
		last := s.Logs[len(s.Logs)-1]
		return last.Term, last.Index
	}
	return s.Logs[next-1-start].Term, next - 1
}

func (s *Server) Do(command CommandEtnry) error {
	logEntry, err := s.WriteLog(command)
	if err != nil {
		return errors.New("write log replica failed")
	}
	err = s.bizApplyFunc(logEntry)
	if err != nil {
		log.Printf("apply log to bussines state machine failed, log:%+v\n", logEntry)
		return errors.New("apply log to bussines state machine failed")
	}
	return nil
}
