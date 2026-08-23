package server

import (
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lwwgo/goraft/util"
)

// 节点角色
type CMRole int

// learener 不参与投票, 也不算在quorum. 只接收 leader 的 append log 请求, 并生成 snapshot
// learner 将本地 snapshot 通过网络 rpc 传送给 leader 和 follower
const (
	Follower CMRole = iota
	Candidate
	Leader
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

type CommandEtnry struct {
	Op   string
	Size int
}

type LogEntry struct {
	Command CommandEtnry
	Term    uint64
	Index   uint64
}

type Server struct {
	// 本节点
	LocalID Peer
	// 集群中其他节点
	Peers []Peer

	// 每个Server既是服务器端, 也是客户端.
	// server接受其他Server的请求
	RpcServer *rpc.Server

	// 状态
	Role CMRole
	// 节点所处的任期
	Term uint64
	// 节点上内存态的命令日志, 待刷入持久化存储
	Logs []LogEntry
	// 投票支持的节点. 空：未投票
	VotedFor Peer
	// 已提交的最新日志编号
	CommittedIndex uint64
	// 已应用到业务逻辑状态机中的最新日志索引
	AppliedIndex uint64
	// 选举超时时间, 开始时间. 每次超时检测完成后, 重置
	ElectionTimeStart time.Time
	// 超时时间间隔, now - ElectionTimeStart > TimeOut, 节点开始发起选举投票
	TimeOut time.Duration
	// 超时时间随机因子
	TimeOutRandomFactor float64
	// 节点状态锁
	MuLock sync.Mutex

	// WAL 日志存储
	WAL *WAL
	// 快照处理器
	Snap *Snapshotter
	// 业务状态机处理回调函数
	bizApplyFunc func(logEntry LogEntry) error
	// 业务状态机的快照接口, 业务层实现快照数据的生成
	getSnapshot func() *Snapshot
	// 日志中最新apply index 和 日志开头apply index的差值达到
	// MaxIndexSpan, 则可以做快照
	MaxIndexSpan uint64
	// 是否在做快照中
	IsSnaping bool

	// ************* leader 仅有的字段 *******
	// leader记录其他每个Server应该接受的下个日志编号
	NextIndex map[string]uint64
}

type Config struct {
	LocalID      string   `toml:"localID"`
	IsLearner    bool     `toml:"isLearner"`
	Peers        []string `toml:"peers"`
	Learner      string   `toml:"learner"`
	WalDir       string   `toml:"walDir"`
	SnapDir      string   `toml:"snapDir"`
	MaxIndexSpan uint64   `toml:"maxIndexSpan"`
	LogDir       string   `toml:"logDir"`
}

func InitServer(conf Config) (*Server, error) {
	peers := make([]Peer, 0, len(conf.Peers)+1)
	for _, addr := range conf.Peers {
		// 本实现里 Peers 的语义是"集群中其他节点", conf 里如果误写了自己要剔除,
		// 否则对自己的 self-RPC 既拿不到票/复制成功, 还会抬高多数派分母导致选不出 leader.
		if addr == conf.LocalID {
			continue
		}
		peers = append(peers, Peer{Addr: addr})
	}
	if len(conf.Learner) > 0 && conf.Learner != conf.LocalID {
		peers = append(peers, Peer{Addr: conf.Learner, Role: Learner})
	}

	role := Follower
	if conf.IsLearner {
		role = Learner
	}

	s := &Server{
		LocalID:             Peer{Addr: conf.LocalID},
		Peers:               peers,
		Role:                role,
		Term:                0,
		Logs:                make([]LogEntry, 0),
		VotedFor:            Peer{},
		CommittedIndex:      0,
		AppliedIndex:        0,
		ElectionTimeStart:   time.Now(),
		TimeOut:             10 * time.Second,
		TimeOutRandomFactor: 0.1,
		NextIndex:           make(map[string]uint64, len(peers)),
		WAL:                 NewWAL(0, 0, conf.WalDir),
		Snap:                NewSnap(0, 0, conf.SnapDir),
		bizApplyFunc: func(logEntry LogEntry) error {
			log.Printf("[example] bussines state machine apply succ")
			return nil
		},
		getSnapshot: func() *Snapshot {
			log.Printf("[example] bussines state machine mirror data")
			kvStore := map[string]int{
				"key1": 1,
				"key2": 2,
			}
			data, _ := json.Marshal(kvStore)
			return &Snapshot{Data: data}
		},
		MaxIndexSpan: conf.MaxIndexSpan,
	}

	// 加载snapshot到业务状态机
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
			log.Printf("reload snapshot file:%s succ, apply index:%d, term:%d\n", filepath, s.AppliedIndex, s.Term)
		}
	}

	// 加载wal到内存
	isExist = util.PathIsExist(s.WAL.WorkPath)
	if !isExist {
		log.Printf("%s does not exist, mkdir it\n", s.WAL.WorkPath)
		os.Mkdir(s.WAL.WorkPath, os.ModePerm)
	}
	files, err = os.ReadDir(s.WAL.WorkPath)
	if err != nil {
		return nil, err
	}

	// 先把所有 wal 文件中的日志全部合并到一个 slice，不依赖文件加载顺序
	var allEntries []LogEntry
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
		// 按 Index 升序排序；同 Index 下 Term 大的更权威（新任期覆盖旧）
		sort.Slice(allEntries, func(i, j int) bool {
			if allEntries[i].Index != allEntries[j].Index {
				return allEntries[i].Index < allEntries[j].Index
			}
			return allEntries[i].Term > allEntries[j].Term
		})
		// 同 Index 去重：此时因为排序规则 Term 大的在前，保留第一条即可
		unique := make([]LogEntry, 0, len(allEntries))
		for i, e := range allEntries {
			if i == 0 || e.Index != unique[len(unique)-1].Index {
				unique = append(unique, e)
			}
		}
		s.Logs = append(s.Logs, unique...)
		if last := s.Logs[len(s.Logs)-1].Term; last > s.Term {
			s.Term = last
		}
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

	return s, nil
}

// 阻塞运行, 直到节点退出
func (s *Server) Run() {
	log.Printf("start to run raft, begin role:%s\n", s.Role.String())
	if s.Role == Learner {
		for {
			time.Sleep(10 * time.Second)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.RunElectionTimer()
	}()

	go func() {
		defer wg.Done()
		s.RunHeartbeatTimer()
		log.Printf("exist heartbeat loop\n")
	}()

	// 测试
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Second)
		log.Printf("this node is %s\n", s.Role.String())
		if s.Role == Leader {
			for i := 0; i < 100; i++ {
				s.Do(CommandEtnry{Op: "write raft log test", Size: i})
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

func (s *Server) MaybeStartSnap() bool {
	if s.Role != Learner || s.IsSnaping || len(s.Logs) == 0 {
		return false
	}
	return s.AppliedIndex-s.Logs[0].Index >= s.MaxIndexSpan
}
