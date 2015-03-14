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

const Debug = 0

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
	Type      OpType
	Key       string
	Value     string
	Client    int
	ClientSeq int
}

type OpReq struct {
	op        Op
	replyChan chan string
}

//As we essentially need to suggest log entries sequentially; because
//we need to avoid duplicates my solution has one single goroutine
//that applies operations sequentially. It gets new operations through a
//channel or by "pinging" paxos

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	lastDummySeq int //Seq of last time we launched a dummy op to fill a hole
	database     map[string]string
	clientSeqs   map[int]int
	opReqChan    chan OpReq
}

func (kv *KVPaxos) sequentialApplier() {

	seq := 1
	for !kv.isdead() {
		select {
		case opreq := <-kv.opReqChan:
			op := opreq.op
			kv.Logf("Got operation through channel")
			seq = kv.addToPaxos(seq, op)
			kv.Logf("Operation added to paxos log at %d", seq)

			if opreq.op.Type == Get {
				kv.Logf("Get applied! Feeding value through channel. %d", seq)
				opreq.replyChan <- kv.database[op.Key]
			} else {
				opreq.replyChan <- "yey"
			}

		case <-time.After(50 * time.Millisecond):
			kv.Logf("Ping")
			seq = kv.ping(seq)
		}

		kv.Logf("Calling Done(%d)", seq-2)
		kv.px.Done(seq - 1)
	}

}

//Takes the last non-applied seq and returns the new one
func (kv *KVPaxos) ping(seq int) int {
	dummyOp := Op{Type: Get}
	for !kv.isdead() {
		fate, val := kv.px.Status(seq)

		if fate == paxos.Decided {
			kv.applyOp(val.(Op))
			seq++
			continue
		}

		if kv.px.Max() > seq && seq > kv.lastDummySeq {
			kv.px.Start(seq, dummyOp)
			kv.waitForPaxos(seq)
			kv.lastDummySeq = seq
		} else {
			return seq
		}

	}
	kv.Logf("ERRRRORR: Ping fallthrough, we are dying! Return seq -1 ")
	return -1

}

func (kv *KVPaxos) addToPaxos(seq int, op Op) (retseq int) {
	for !kv.isdead() {
		//Suggest OP as next seq

		if op.Type != Get && op.ClientSeq <= kv.clientSeqs[op.Client] {
			//Duplicate! Ignore it and return   (Don't mind dup gets)
			kv.Logf("Ignoring duplicate Put/Append")
			return seq
		}

		kv.px.Start(seq, op)
		val, err := kv.waitForPaxos(seq)

		if err != nil {
			kv.Logf("ERRRROROOROROO!!!")
			continue
		}

		kv.applyOp(val.(Op))

		seq++

		//Did work?
		if val == op {
			kv.Logf("Applied operation in log at seq %d", seq-1)
			return seq
		} else {
			kv.Logf("Somebody else took seq %d before us, applying it and trying again", seq-1)
		}
	}
	return -1

}

func (kv *KVPaxos) applyOp(op Op) {
	//Note, don't update clientseq outside conditionals,
	//as there is no guarantee Gets can't be out of order
	if op.Type == Put {
		kv.clientSeqs[op.Client] = op.ClientSeq
		kv.Logf("Applying put(%s) to database", op.Key)
		kv.database[op.Key] = op.Value
	} else if op.Type == Append {
		kv.clientSeqs[op.Client] = op.ClientSeq
		kv.Logf("Applying append(%s) to database", op.Key)
		kv.database[op.Key] = kv.database[op.Key] + op.Value
	}

	//Do nothing for get
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.Logf("Server Get!")

	op := Op{Type: Get, Key: args.Key, Client: args.Client, ClientSeq: args.ClientSeq}

	opReq := OpReq{op, make(chan string, 1)}

	kv.opReqChan <- opReq

	reply.Value = <-opReq.replyChan
	kv.Logf("Got Get reply, returning to client!")
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.Logf("Server PutAppend!")

	op := Op{Type: args.Op, Key: args.Key,
		Value: args.Value, Client: args.Client, ClientSeq: args.ClientSeq}

	opReq := OpReq{op, make(chan string, 1)}

	kv.opReqChan <- opReq
	<-opReq.replyChan
	return nil
}

func (kv *KVPaxos) waitForPaxos(seq int) (val interface{}, err error) {
	var status paxos.Fate
	to := 10 * time.Millisecond
	for {
		status, val = kv.px.Status(seq)
		if status == paxos.Decided {
			err = nil
			return
		}

		if status == paxos.Forgotten || kv.isdead() {
			err = fmt.Errorf("We are dead or waiting for something forgotten. Server shutting down?")
			kv.Logf("We are dead or waiting for something forgotten. Server shutting down?")
			return
		}

		kv.Logf("Still waiting for paxos: %d", seq)
		time.Sleep(to)
		if to < 3*time.Second {
			to *= 2
		} else {
			err = fmt.Errorf("Wait for paxos timeout!1")
			return
		}
	}

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
	kv.clientSeqs = make(map[int]int)
	kv.opReqChan = make(chan OpReq)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go kv.sequentialApplier()

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
