package pbservice

const (
	OK             = ""
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string
    UID     int64
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
    UID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferDataArgs struct {
	Data map[string]string
    Executed map[int64]bool
	// You'll have to add definitions here.
}

type TransferDataReply struct {
	Err   Err
}

// Your RPC definitions here.
