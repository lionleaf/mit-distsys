package kvpaxos

import "fmt"

const debug = false

func Printf(format string, a ...interface{}) {
	if !debug {
		return
	}

	fmt.Printf(format+"\n", a...)

}

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type OpType int

const (
	Get OpType = iota + 1
	Put
	Append
)

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
