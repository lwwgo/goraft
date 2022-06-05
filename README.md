# goraft
Implementing raft protocol with golang

# 项目文档
https://pkg.go.dev/github.com/lwwgo/goraft

# 关键流程
## leader写流程
1、写本地内存  
2、写本地wal  
3、日志并发发送给其他followeer节点
4、过半peer返回append log entry成功 =》 标记日志已提交，apply到业务状态机，调整applied index，返回给client succ；
    过半peer返回append log entry失败/超时 =》 回滚本地内存日志，返回给client fail
## follower写流程
1、收到append log entry请求，进行日志一致性检查
2、不满足一致性检查，则删除本机最后一条日志（内存态），返回leader append命令失败
3、写本地内存
4、写本地wal
5、返回append log命令成功
6、在下一次leader 发来心跳请求中，检查leaderCommitted是否大于本节点 committedIndex，若大于本节点提交索引，则将本节点leaderCommitted位置log标记为已提交

## leader宕机处理
### 日志安全约束
leader 只commit本任期内日志，但是apply所有已在本节点日志中，且未apply的日志。
### 第一种：leader 已commit、apply日志，并返回client succ，在下一次心跳前，leader崩溃
新leader任期内，由于【日志安全约束】本条日志不再单独commit，但是当新任期中新日志commit时，之前的日志 顺带 被commit。但老日志未apply的还是需要apply，
因为如果不在新leader中apply旧任期的日志，则新leader 业务状态机中会丢失已经返回给client succ的元信息，导致后面信息错乱。
### 第二种：leader 未commit、apply日志，也没返回client succ，在下一次心跳前，leader崩溃
与第一种情况不同的处理地方是：此时新leader已经无法辨别老leader是否已经返回client succ，只能"委屈"client了，让client在超时时，重发命令，探测下上一个超时
命令是否已经完成，且client命令中需要携带一个字段：命令的唯一标识(可以用分布式ID，雪花算法)，且leader 能够做幂等检查。
以上两种情况，都是在过半peer节点均在本地成功写入内存、写入wal下，其他情况都较为简单，不在此一一列举。

# 参考文档
raft简介: https://www.cnblogs.com/richaaaard/p/6351705.html  

raft中文论文: https://www.cnblogs.com/linbingdong/p/6442673.html  
