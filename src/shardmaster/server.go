package shardmaster

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

const Debug = true

var log_mu sync.Mutex

func (sm *ShardMaster) Logf(format string, a ...interface{}) {
	if !Debug {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := sm.me

	fmt.Printf("\x1b[%dm", (me%6)+31)
	fmt.Printf("SM#%d : ", me)
	fmt.Printf(format+"\n", a...)
	fmt.Printf("\x1b[0m")
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	opReqChan    chan OpReq
	lastDummySeq int //Seq of last time we launched a dummy op to fill a hole

	configs []Config // indexed by config num
}

type Op struct {
	OpID    int64
	Type    OpType
	GID     int64    //Used by all Ops but Query
	Servers []string //Used by Join
	Shard   int      //Used by move
	Num     int      //Used by Query
}

type OpType int

const (
	Join OpType = iota + 1
	Leave
	Move
	Query
	Dummy
)

type OpReq struct {
	op        Op
	replyChan chan string
}

func (sm *ShardMaster) sequentialApplier() {

	seq := 1
	for !sm.isdead() {
		select {
		case opreq := <-sm.opReqChan:
			op := opreq.op
			sm.Logf("Got operation through channel")
			seq = sm.addToPaxos(seq, op)
			sm.Logf("Operation added to paxos log at %d", seq)

			/* TODO remove
			if opreq.op.Type == Get {
				kv.Logf("Get applied! Feeding value through channel. %d", seq)
				opreq.replyChan <- kv.database[op.Key]
			} else {
				opreq.replyChan <- "yey"
			}*/

		case <-time.After(50 * time.Millisecond):
			sm.Logf("Ping")
			seq = sm.ping(seq)
		}

		sm.Logf("Calling Done(%d)", seq-2)
		sm.px.Done(seq - 1)
	}

}

//Takes the last non-applied seq and returns the new one
func (sm *ShardMaster) ping(seq int) int {
	//TODO: Is this a good dummy OP?
	dummyOp := Op{}

	for !sm.isdead() {
		fate, val := sm.px.Status(seq)

		if fate == paxos.Decided {
			sm.applyOp(val.(Op))
			seq++
			continue
		}

		if sm.px.Max() > seq && seq > sm.lastDummySeq {
			sm.px.Start(seq, dummyOp)
			sm.waitForPaxos(seq)
			sm.lastDummySeq = seq
		} else {
			return seq
		}

	}
	sm.Logf("ERRRRORR: Ping fallthrough, we are dying! Return seq -1 ")
	return -1

}

func (sm *ShardMaster) addToPaxos(seq int, op Op) (retseq int) {
	for !sm.isdead() {
		//Suggest OP as next seq
		sm.px.Start(seq, op)

		val, err := sm.waitForPaxos(seq)

		if err != nil {
			sm.Logf("ERRRROROOROROO!!!")
			continue
		}

		sm.applyOp(val.(Op))

		seq++

		//Did work?
		if val.(Op).O == op {
			sm.Logf("Applied operation in log at seq %d", seq-1)
			return seq
		} else {
			sm.Logf("Somebody else took seq %d before us, applying it and trying again", seq-1)
		}
	}
	return -1

}

func (sm *ShardMaster) waitForPaxos(seq int) (val interface{}, err error) {
	var status paxos.Fate
	to := 10 * time.Millisecond
	for {
		status, val = sm.px.Status(seq)
		if status == paxos.Decided {
			err = nil
			return
		}

		if status == paxos.Forgotten || sm.isdead() {
			err = fmt.Errorf("We are dead or waiting for something forgotten. Server shutting down?")
			sm.Logf("We are dead or waiting for something forgotten. Server shutting down?")
			return
		}

		sm.Logf("Still waiting for paxos: %d", seq)
		time.Sleep(to)
		if to < 3*time.Second {
			to *= 2
		} else {
			err = fmt.Errorf("Wait for paxos timeout!1")
			return
		}
	}

}

func (sm *ShardMaster) applyOp(op Op) {
	sm.Logf("Applying op to database")
	switch op.Type {
	case Join:
		sm.Logf("Join, you guys!")
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {

	op := Op{GID: args.GID, Servers: args.Servers}

	opReq := OpReq{op, make(chan string, 1)}

	sm.opReqChan <- opReq
	<-opReq.replyChan
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.opReqChan = make(chan OpReq)

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	go sm.sequentialApplier()

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
