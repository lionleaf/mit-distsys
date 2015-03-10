package kvpaxos

import "fmt"

const debug = true

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
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
