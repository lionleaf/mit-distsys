package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type OpType int

const (
	Get OpType = iota + 1
	Put
	Append
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        OpType // "Put" or "Append"
	Client    int
	ClientSeq int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	Client    int
	ClientSeq int
}

type GetReply struct {
	Err   Err
	Value string
}
