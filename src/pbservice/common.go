package pbservice

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

const debug = true

func DebugPrintf(str string, v ...interface{}){
    if debug {
        fmt.Printf(str, v...)
    }
}

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string
    UID     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
    UID int64
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferDataArgs struct {
	Data map[string]string
    Executed map[int64]bool
}

type TransferDataReply struct {
	Err   Err
}
