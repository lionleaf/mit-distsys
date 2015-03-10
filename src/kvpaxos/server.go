package kvpaxos

import (
	"net"
	"time"
)
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

const Debug = 1

var log_mu sync.Mutex

func (kv *KVPaxos) Logf(format string, a ...interface{}) {
	if Debug <= 0 {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := kv.me

	fmt.Printf("\x1b[%dm", (me%6)+31)
	fmt.Printf("S#%d : ", me)
	fmt.Printf(format+"\n", a...)
	fmt.Printf("\x1b[0m")
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type   OpType
	Key    string
	Value  string
	Opnr   int
	Server int //The server that issued this operation

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

//One goroutine will continually try to apply operations
//to the database in order after they have been decided by paxos
//This way PutAppend can return right after the operation has been
//successfully added to the Paxos log.
//A get request works as follows:
//Register a new channel in getRequestChannels for the seq we're
//trying to get paxos to accept. (If we fail, remove channel and add new one with the new seq)
//Listen on the channel for the return value of the get
//The automatic applier will send return values of get ops from this channel
//through the appropriate channel.

//It is important that the channel is registered before paxos is called as the applier might get
//the confirmed operation from paxos before the Get goroutine knows it has completed.

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	getRequestChannels map[int]chan string

	seq           int
	nextCommitSeq int
	opnr          int //Unique number per op per server.
	lock          sync.Mutex

	database map[string]string
}

func (kv *KVPaxos) applyLoop() {
	var seq int
	kv.nextCommitSeq = 1 //Because I use paxos.Max() + 1, first seq is 1
	for !kv.isdead() {
		seq = kv.nextCommitSeq

		fate, val := kv.px.Status(seq)
		if fate != paxos.Decided {
			//TODO: Exponential stepback?
			//On demand ping?
			time.Sleep(20 * time.Millisecond)
			continue
		}

		kv.Logf("Applying operation to database!")
		op := val.(Op)

		switch op.Type {
		case Put:
			kv.database[op.Key] = op.Value
		case Append:
			kv.database[op.Key] = kv.database[op.Key] + op.Value
		case Get:
			kv.Logf("Get request! server: %d, me: %d ", op.Server, kv.me)
			if op.Server == kv.me {
				kv.lock.Lock()
				channel := kv.getRequestChannels[seq]
				kv.lock.Unlock()
				kv.Logf("Sending get value through channel: %d", seq)
				channel <- kv.database[op.Key]
			}
		}

		kv.nextCommitSeq++
	}
}

func (kv *KVPaxos) nextOpNr() int {
	kv.lock.Lock()
	defer kv.lock.Unlock()
	kv.opnr++
	return kv.opnr
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {

	seq := kv.AddLogEntry(Get, args.Key, "")

	kv.lock.Lock()
	returnChan := kv.getRequestChannels[seq]
	kv.lock.Unlock()

	kv.Logf("Get added to log! Waiting on channel %d", seq)
	reply.Value = <-returnChan

	kv.Logf("Channel returned get value")
	kv.lock.Lock()
	delete(kv.getRequestChannels, seq)
	kv.lock.Unlock()

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.Logf("Server PutAppend!")
	kv.AddLogEntry(args.Op, args.Key, args.Value)
	return nil
}

func (kv *KVPaxos) AddLogEntry(op OpType, key string, val string) (seq int) {
	nr := kv.nextOpNr()
	logEntry := Op{Type: op, Key: key, Value: val, Opnr: nr, Server: kv.me}

	for !kv.isdead() {

		seq := kv.px.Max() + 1

		if op == Get {
			kv.lock.Lock()
			if _, ok := kv.getRequestChannels[seq]; ok {
				//Somebody else is already trying a get with this seq, better give up.
				kv.lock.Unlock()
				continue
			}
			channel := make(chan string)
			kv.Logf("Adding get channel at %d", seq)
			kv.getRequestChannels[seq] = channel
			kv.lock.Unlock()
		}

		kv.px.Start(seq, logEntry)
		kv.Logf("Starting %d", seq)

		time.Sleep(100 * time.Millisecond)

		var fate paxos.Fate
		var val interface{}
		for fate, val = kv.px.Status(seq); fate != paxos.Decided; {
			time.Sleep(time.Millisecond)
		}

		kv.Logf("Decided!")
		if val == logEntry {
			kv.Logf("Won!")
			return seq
		} else {
			kv.Logf("Other operation first!")
			if op == Get {
				kv.lock.Lock()
				kv.Logf("Removing channel!")
				delete(kv.getRequestChannels, seq)
				kv.lock.Unlock()
			}
		}
	}
	return -1

}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.database = make(map[string]string)

	kv.getRequestChannels = make(map[int]chan string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go kv.applyLoop()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
