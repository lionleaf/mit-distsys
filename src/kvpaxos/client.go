package kvpaxos

import (
	"net/rpc"
	"sync"
)
import "crypto/rand"
import "math/big"

import "fmt"

type Clerk struct {
	servers []string
	me      int
	nextSeq int
	mutex   sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var nextClerkNumber int
var clerkNrLock sync.Mutex

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.nextSeq = 1

	clerkNrLock.Lock()
	ck.me = nextClerkNumber
	nextClerkNumber++
	clerkNrLock.Unlock()

	Printf("New clerk with number: %d", ck.me)

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) nextSeqNr() int {

	ck.lock()
	seq := ck.nextSeq
	ck.nextSeq++
	ck.unlock()

	return seq
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	seq := ck.nextSeqNr()

	args := GetArgs{Key: key, Client: ck.me, ClientSeq: seq}
	reply := GetReply{}

	ok := false
	for !ok {
		server := int(nrand()) % len(ck.servers)
		Printf("Calling Get on server: %d (clientID, clientSeq): (%d,%d)", server, ck.me, seq)
		ok = call(ck.servers[server], "KVPaxos.Get", args, &reply)
		Printf("Get returned from server: %d clientSeq: (%d, %d)  OK: %t", server, ck.me, seq, ok)
	}
	Printf("Get return (client, seq) (%d, %d)!", ck.me, seq)
	return reply.Value
}

func (ck *Clerk) lock() {
	ck.mutex.Lock()
}

func (ck *Clerk) unlock() {
	ck.mutex.Unlock()
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {

	seq := ck.nextSeqNr()

	args := PutAppendArgs{Key: key, Value: value, Op: op, Client: ck.me, ClientSeq: seq}
	reply := PutAppendReply{}

	ok := false
	for !ok {
		server := int(nrand()) % len(ck.servers)
		Printf("Calling PutAppend on server: %d (clientID, clientSeq): (%d,%d)", server, ck.me, seq)
		ok = call(ck.servers[server], "KVPaxos.PutAppend", args, &reply)
	}
	Printf("Append return %d!", seq)
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	Printf("Put!")
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	Printf("Append!")
	ck.PutAppend(key, value, Append)
}
