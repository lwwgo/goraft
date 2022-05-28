package server

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"raft/util"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Peer struct {
	Addr string
}

func (p Peer) Empty() bool {
	return reflect.DeepEqual(p, Peer{})
}

func (p Peer) Equal(x Peer) bool {
	return p.Addr == x.Addr
}

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Dead:
		return "dead"
	default:
		return "unsupported"
	}
}

type CommandEtnry struct {
	Op   string
	Size int
}

type LogEntry struct {
	Command CommandEtnry
	Term    int
}

type Server struct {
	// 本节点
	LocalID Peer
	// 集群中其他节点
	Peers []Peer

	// 每个Server既是服务器端, 也是客户端.
	// server接受其他Server的请求, client向其他Server发送请求
	RpcServer  *rpc.Server
	RpcClients map[string]*rpc.Client

	// 状态
	State CMState
	// 节点所处的任期
	Term int
	// 节点上内存态的命令日志, 待刷入持久化存储
	Logs []LogEntry
	// 投票支持的节点. 空：未投票
	VotedFor Peer
	// 已提交的最新日志编号
	CommitIndex int
	// 选举超时时间, 开始时间. 每次超时检测完成后, 重置
	ElectionTimeStart time.Time
	// 超时时间间隔, now - ElectionTimeStart > TimeOut, 节点开始发起选举投票
	TimeOut time.Duration
	// 超时时间随机因子
	TimeOutRandomFactor float64
	// 节点状态锁
	MuLock sync.Mutex

	// ************* leader 仅有的字段 *******
	// leader记录其他每个Server应该接受的下个日志编号
	NextIndex map[string]int
}

// 投票请求
type RequestVote struct {
	// 发起投票请求节点的当前任期号
	Term        int
	CandidateID Peer
	// 发起投票节点在日志中的最后任期号
	Lastterm int
	// 发起投票节点在日志中的最后编号
	LastIndex int
}

// 投票响应
type ResponseVote struct {
	// 接收节点所在的任期
	Term int
	// true: 赞成; false: 反对
	VoteGranted bool
}

func InitServer(addr string, peerAddrs []string) (*Server, error) {
	peers := make([]Peer, 0)
	for _, addr := range peerAddrs {
		peers = append(peers, Peer{Addr: addr})
	}
	s := &Server{
		LocalID:             Peer{Addr: addr},
		Peers:               peers,
		RpcClients:          make(map[string]*rpc.Client, len(peers)),
		State:               Follower,
		Term:                0,
		Logs:                make([]LogEntry, 1),
		VotedFor:            Peer{},
		CommitIndex:         0,
		ElectionTimeStart:   time.Now(),
		TimeOut:             10 * time.Second,
		TimeOutRandomFactor: 0.1,
		NextIndex:           make(map[string]int, len(peers)),
	}
	s.Logs[0] = LogEntry{
		Command: CommandEtnry{Op: "log-head", Size: -1},
		// 0 是一个不会出现在真是任期的任期号, 真实任期从1开始标号.
		Term: 0,
	}
	for _, peer := range peers {
		s.NextIndex[peer.Addr] = len(s.Logs)
	}
	log.Printf("after init server, nextIndex: %+v\n", s.NextIndex)

	rpc.RegisterName("Server", s)
	listener, err := net.Listen("tcp", s.Addr())
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			rpc.ServeConn(conn)
		}
	}()

	for _, peer := range peers {
		if _, ok := s.RpcClients[peer.Addr]; !ok {
			client, err := rpc.Dial("tcp", peer.Addr)
			if err != nil {
				panic(err)
			}
			s.RpcClients[peer.Addr] = client
		}
	}
	return s, nil
}

func (nd *Server) Addr() string {
	return nd.LocalID.Addr
}

func (nd *Server) VoteHandler(req RequestVote, resp *ResponseVote) error {
	log.Printf("receive vote request:%+v\n", req)
	nd.MuLock.Lock()
	defer nd.MuLock.Unlock()

	resp.VoteGranted = false
	LastTerm := nd.getLogTerm()
	lastIndex := nd.getLogIndex()
	// 接收到的任期 > 节点任期, 并且节点没有投过票或者在上一轮投票给了接收到的节点, 并且接收到的节点日志比本节点日志新
	// Raft 通过比较两份日志中最后一条日志条目的索引值和任期号来定义谁的日志比较新
	// 如果两份日志最后条目的任期号不同，那么任期号大的日志更新
	// 如果两份日志最后条目的任期号相同，那么日志较长的那个更新。
	if req.Term > nd.Term &&
		(nd.VotedFor.Empty() || nd.VotedFor.Equal(req.CandidateID)) &&
		(req.Lastterm > LastTerm || (req.Lastterm == LastTerm && req.LastIndex >= lastIndex)) {
		log.Printf("receive vote request from %s, Server[%s] change state from %s to %s\n",
			req.CandidateID, nd.LocalID, nd.State.String(), Follower.String())
		nd.State = Follower
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
		Term:        nd.Term,
		CandidateID: nd.LocalID,
		LastIndex:   nd.getLogIndex(),
		Lastterm:    nd.getLogTerm(),
	}
	for _, peer := range nd.Peers {
		if nd.State != Candidate {
			return
		}

		wg.Add(1)
		go func(peer Peer) {
			defer wg.Done()
			response := &ResponseVote{}
			err := util.RpcCallTimeout(nd.RpcClients[peer.Addr], "Server.VoteHandler", request, response, 2*time.Second)
			if err != nil {
				log.Printf("rpc client send request failed, err:%s\n", err.Error())
			}
			if response.VoteGranted {
				atomic.AddInt64(&winCount, 1)
			} else {
				log.Printf("vote request failed, voteGranted:%v, \n", response.VoteGranted)
			}
			// 选票超过集群中节点数量的一半, 则当选
			if int(winCount*2) > len(nd.Peers)+1 {
				// 成为主节点
				nd.MuLock.Lock()
				nd.State = Leader
				nd.VotedFor = Peer{}
				nd.MuLock.Unlock()
				log.Printf("Server[%s] won the election, become to be leader, winCount:%d, sum:%d\n", nd.LocalID.Addr, winCount, len(nd.Peers)+1)

				// 通过心跳, 通知其他从节点结束本轮选举
				nd.SendHeartbeat()
				return
			}
		}(peer)
	}

	wg.Wait()
	if int(winCount*2) <= len(nd.Peers)+1 {
		log.Printf("Server[%s] lost the election, winCount:%d, sum:%d\n", nd.LocalID.Addr, winCount, len(nd.Peers)+1)
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
		if nd.State == Leader {
			continue
		}

		if time.Since(nd.ElectionTimeStart) >= nd.timeOutInternal() {
			// 选举超时, 成为候选节点, 首先增加任期号, 并投自己一票
			nd.MuLock.Lock()
			nd.State = Candidate
			nd.Term++
			nd.VotedFor = nd.LocalID
			nd.ElectionTimeStart = time.Now()
			nd.MuLock.Unlock()

			// 向其他节点发起选举请求
			log.Printf("Server[%s] began to launch an election\n", nd.LocalID.Addr)
			nd.Elect()
			log.Printf("the election Server[%s] initiated is over\n", nd.LocalID.Addr)
		}
	}
}

func (nd *Server) getLogIndex() int {
	if len(nd.Logs) == 0 {
		return -1
	}
	return len(nd.Logs) - 1
}

func (nd *Server) getLogTerm() int {
	if len(nd.Logs) == 0 {
		return 0
	}
	return nd.Logs[len(nd.Logs)-1].Term
}

// 发送一次心跳
func (s *Server) SendHeartbeat() {
	// 只有主才会向其他节点发送心跳
	if s.State != Leader {
		return
	}

	log.Printf("leader[%s] start to send heartbeat\n", s.LocalID.Addr)
	requestAppend := &RequestAppend{
		Term:        s.Term,
		LeaderID:    s.LocalID,
		PreLogIndex: s.getLogIndex(),
		PreLogTerm:  s.getLogTerm(),
	}
	for _, peer := range s.Peers {
		go func(peer Peer) {
			responseAppend := &ResponseAppend{}
			if err := s.RpcClients[peer.Addr].Call("Server.AppendEntryHandler", requestAppend, responseAppend); err != nil {
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
		if nd.State == Leader {
			// 向其他节点发起选举请求
			nd.SendHeartbeat()
		}
	}
}

// 阻塞运行, 直到节点退出
func (nd *Server) Run() {
	log.Printf("start to run raft\n")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		nd.RunElectionTimer()
	}()

	go func() {
		defer wg.Done()
		nd.RunHeartbeatTimer()
	}()

	// 测试
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Second)
		log.Printf("this node is %s\n", nd.State.String())
		if nd.State == Leader {
			for i := 0; i < 100; i++ {
				nd.WriteLog(CommandEtnry{Op: "write raft log test", Size: i})
				time.Sleep(5 * time.Second)
			}
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			buff := make([]byte, 1<<10)
			runtime.Stack(buff, false)
			log.Printf("%v %v\n", r, string(buff))
		}
	}()
	wg.Wait()
}
