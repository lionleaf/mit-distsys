package shardkv

import "net"
import "fmt"
import (
	"net/http"
	"net/rpc"
)
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

//Debugging: TODO: remove
import _ "net/http/pprof"

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	lastDummySeq int //Seq of last time we launched a dummy op to fill a hole
	database     map[string]string
	clientSeqs   map[int]int
	opReqChan    chan OpReq

	currentConfig shardmaster.Config
	myShard       map[int]bool //I'm responsible for a shard if myShard[shardNr] == true
}

const Debug = 1

var log_mu sync.Mutex

func (kv *ShardKV) Logf(format string, a ...interface{}) {
	if Debug <= 0 {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := kv.me

	fmt.Printf("\x1b[%dm", ((me+int(kv.gid))%9)+31)
	fmt.Printf("S#%d@%d : ", me, kv.gid)
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
	Config    shardmaster.Config
}

// For config OPs, set ClientSeq = Config.Num and Client = 0

type OpReq struct {
	op        Op
	replyChan chan string
	errChan   chan Err
}

func (kv *ShardKV) sequentialApplier() {
	seq := 1
	for !kv.isdead() {
		select {
		case opreq := <-kv.opReqChan:
			op := opreq.op
			kv.Logf("Got operation through channel")

			if op.Type != NewConfig && !kv.myShard[key2shard(op.Key)] {
				opreq.replyChan <- ""
				opreq.errChan <- ErrWrongGroup
				kv.Logf("Key from a shard that is not mine! Key: %s ", op.Key)

				//In case we get flooded with wrong keys we need to keep up to date
				kv.Logf("Ping")
				seq = kv.ping(seq)
				break
			}

			seq = kv.addToPaxos(seq, op)

			kv.Logf("Operation added to paxos log at %d", seq)

			if opreq.op.Type == Get {
				kv.Logf("Get applied! Feeding value through channel. %d", seq)
				opreq.replyChan <- kv.database[op.Key]
				opreq.errChan <- OK
			} else {
				opreq.replyChan <- "yey"
				opreq.errChan <- OK
			}

		case <-time.After(50 * time.Millisecond):
			kv.Logf("Ping")
			seq = kv.ping(seq)
		}

		kv.Logf("Calling Done(%d)", seq-2)
		kv.px.Done(seq - 1)
	}
}

//Handle a new config and freeze(don't return) until we added all new keys
func (kv *ShardKV) newConfigFreeze(newConfig shardmaster.Config) {
	kv.Logf("New config! Freeze until everything is sorted")

	newShards := make(map[int]bool)

	for shard, gid := range newConfig.Shards {
		//TODO: Handle transfers
		if gid == kv.gid {
			if !kv.myShard[shard] {
				newShards[shard] = true
				kv.myShard[shard] = true
			}
		} else {
			kv.myShard[shard] = false
		}
	}

	oldConfig := kv.currentConfig

	for shard, _ := range newShards {
		kv.fetchShardFromGroup(oldConfig.Shards[shard])
	}

	kv.currentConfig = newConfig
}

//Takes the last non-applied seq and returns the new one
func (kv *ShardKV) ping(seq int) int {
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

func (kv *ShardKV) addToPaxos(seq int, op Op) (retseq int) {
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
		if val.(Op).Client == op.Client && val.(Op).ClientSeq == op.ClientSeq {
			kv.Logf("Applied operation in log at seq %d", seq-1)
			return seq
		} else {
			kv.Logf("Somebody else took seq %d before us, applying it and trying again", seq-1)
		}
	}
	return -1
}
func (kv *ShardKV) applyOp(op Op) {
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
	} else if op.Type == NewConfig {
		kv.newConfigFreeze(op.Config)
	}

	//Do nothing for get
}

func (kv *ShardKV) fetchShardFromGroup(shard int, gid int64) {

	args := &GetShardArgs{}
	args.Shard = shard
	args.ConfigNr = curConfig.Num
	var reply GetShardReply
	for {
		servers, ok := ck.config.Groups[gid]

		if ok {
			// try each server in the shard's replication group.
			for _, srv := range servers {
				ok := call(srv, "ShardKV.GetShard", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					applyShardOps(reply.Ops)
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
	return

}

func (kv *ShardKV) applyShardOps(ops []interface{}) {
	for op := range ops {
		kv.applyOp(ops.(Op))
	}

}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {

	//TODO: Freeze everything when responding? Yes? OMG, can I deadlock?!

	if kv.currentConfig.Num != args.ConfigNr {
		reply.Err = ErrWrongGroup
		return nil
	}

	reply.Ops = make([]interface{}, 0)

	for key, val := range kv.database {
		if key2shard(key) == args.Shard {
			reply.Ops = append(reply.Ops, Op{Type: Put, Key: key, Value: val})
		}
	}

	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.Logf("Server Get!")

	op := Op{Type: Get, Key: args.Key, Client: args.Client, ClientSeq: args.ClientSeq}

	opReq := OpReq{op, make(chan string, 1), make(chan Err, 1)}

	kv.opReqChan <- opReq

	reply.Value = <-opReq.replyChan
	reply.Err = <-opReq.errChan
	kv.Logf("Got Get reply, returning to client! Err: %s", reply.Err)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.Logf("Server PutAppend!")

	op := Op{Type: args.Op, Key: args.Key,
		Value: args.Value, Client: args.Client, ClientSeq: args.ClientSeq}

	opReq := OpReq{op, make(chan string, 1), make(chan Err, 1)}

	kv.opReqChan <- opReq

	reply.Err = <-opReq.errChan

	return nil
}

func (kv *ShardKV) waitForPaxos(seq int) (val interface{}, err error) {
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

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	newConfig := kv.sm.Query(-1)
	if newConfig.Num == kv.currentConfig.Num {
		return
	}

	kv.Logf("New configuration!")
	op := Op{Type: NewConfig, Config: newConfig, Client: 0, ClientSeq: newConfig.Num}

	opReq := OpReq{op, make(chan string, 1), make(chan Err, 1)}

	kv.opReqChan <- opReq

}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.database = make(map[string]string)
	kv.clientSeqs = make(map[int]int)
	kv.opReqChan = make(chan OpReq)
	kv.myShard = make(map[int]bool)

	// Your initialization code here.
	// Don't call Join().

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

	//Debugging: TODO: Remove
	go func() {
		log.Println(http.ListenAndServe("localhost:6262", nil))
	}()

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
