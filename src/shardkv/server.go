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

const Debug = 0

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
	nextSeq      int //Next paxos sequence number to use / look for
	database     map[string]string
	clientSeqs   map[int]int
	opReqChan    chan OpReq

	currentConfig shardmaster.Config
	oldConfig     shardmaster.Config
	myShard       map[int]bool //I'm responsible for a shard if myShard[shardNr] == true

	nextConfigNum int

	gotShardChan chan int
}

type Op struct {
	Type      OpType
	Key       string
	Value     string
	Shard     int
	ShardOps  []interface{}
	Client    int
	ClientSeq int
	Config    shardmaster.Config
}

var log_mu sync.Mutex

func (kv *ShardKV) Logf(format string, a ...interface{}) {
	if Debug <= 0 {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := kv.me

	fmt.Printf("\x1b[%dm", ((me*3+int(kv.gid))%6)+31)
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

// For config OPs, set ClientSeq = Config.Num and Client = 0

type OpReq struct {
	op        Op
	replyChan chan string
	errChan   chan Err
}

func (kv *ShardKV) sequentialApplier() {
	kv.nextSeq = 1
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
				kv.ping()
				break
			}

			kv.addToPaxos(op)

			kv.Logf("Operation added to paxos log at %d", kv.nextSeq)

			if opreq.op.Type == Get {
				kv.Logf("Get applied! Feeding value through channel. %d", kv.nextSeq)
				opreq.replyChan <- kv.database[op.Key]
				opreq.errChan <- OK
			} else {
				opreq.replyChan <- "yey"
				opreq.errChan <- OK
			}

		case <-time.After(500 * time.Millisecond):
			kv.Logf("Ping")
			kv.ping()
		}

		kv.Logf("Calling Done(%d)", kv.nextSeq-2)
		kv.px.Done(kv.nextSeq - 1)
	}
}

//Takes the last non-applied seq and returns the new one
func (kv *ShardKV) ping() {
	dummyOp := Op{Type: Get}
	for !kv.isdead() {
		fate, val := kv.px.Status(kv.nextSeq)

		if fate == paxos.Decided {
			kv.nextSeq++
			kv.applyOp(val.(Op))
			continue
		}

		if kv.px.Max() > kv.nextSeq && kv.nextSeq > kv.lastDummySeq {
			kv.px.Start(kv.nextSeq, dummyOp)
			kv.waitForPaxos(kv.nextSeq)
			kv.lastDummySeq = kv.nextSeq
		} else {
			return
		}

	}
	kv.Logf("ERRRRORR: Ping fallthrough, we are dying!")
	return
}

func (kv *ShardKV) addToPaxos(op Op) {
	for !kv.isdead() {
		//Suggest OP as next seq

		if (op.Type == Put || op.Type == Append) && op.ClientSeq <= kv.clientSeqs[op.Client] {
			//Duplicate! Ignore it and return   (Don't mind dup gets)
			kv.Logf("Ignoring duplicate Put/Append")
			return
		}

		kv.px.Start(kv.nextSeq, op)
		val, err := kv.waitForPaxos(kv.nextSeq)

		if err != nil {
			kv.Logf("ERRRROROOROROO!!!")
			continue
		}

		kv.nextSeq++
		kv.applyOp(val.(Op))

		//Did work?
		if val.(Op).Client == op.Client && val.(Op).ClientSeq == op.ClientSeq {
			kv.Logf("Applied operation in log at seq %d", kv.nextSeq-1)
			return
		} else {
			kv.Logf("Somebody else took seq %d before us, applying it and trying again", kv.nextSeq-1)
		}
	}
	return
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
		kv.applyNewConfig(op.Config, op.Client == kv.me)
	}

	//Do nothing for get
}

//Handle a new config and freeze(don't return) until we added all new keys
func (kv *ShardKV) applyNewConfig(newConfig shardmaster.Config, leader bool) {
	if newConfig.Num == kv.currentConfig.Num {
		kv.Logf("Duplicate changeconfig in paxos log")
		return
	}

	kv.Logf("New config! Freeze until everything is sorted")

	newShards := make(map[int]bool) //New shards I got responsibility for
	oldShards := make(map[int]bool) //Shards that I'm no longer responsible for, make sure they get transfered

	for shard, gid := range newConfig.Shards {
		//TODO: Handle transfers
		if gid == kv.gid {
			if !kv.myShard[shard] {
				newShards[shard] = false
				kv.myShard[shard] = true
				kv.Logf("Oooh, I've got responsibility for a new shard: %d", shard)
			}
		} else {
			if kv.myShard[shard] { //If I used to own this shard
				kv.myShard[shard] = false
				oldShards[shard] = true
				kv.Logf("Oh, I lost responsibility for a shard: %d", shard)
			}
		}
	}

	kv.oldConfig = kv.currentConfig
	kv.currentConfig = newConfig

	if newConfig.Num <= 1 {
		kv.Logf("First config, no shards to transfer")
		return
	}

	//TODO: remove
	if len(newShards) == 0 {
		kv.Logf("Don't need other shards, let's just continue for debugging purposes")
		return
	}

	if leader {
		kv.newConfigLeader(newShards, oldShards)
	} else {
		kv.waitForLeaderToGetThemShards(oldShards)
	}

}

type shardResponse struct {
	Shard    []interface{} //Slice of Put Ops that can be directly applied to update this shard
	ShardNum int
}

func allTrue(testmap map[int]bool) bool {
	for _, v := range testmap {
		if !v {
			return false
		}
	}
	return true
}

func (kv *ShardKV) newConfigLeader(newShards map[int]bool, oldShards map[int]bool) {
	//Called by one server per group upon a new config.
	kv.Logf("newConfigLeader")
	defer kv.Logf("return newConfigLeader")

	shardChan := make(chan shardResponse)
	stopChan := make(chan bool)

	//Start a request for all the missing shards
	for shard, _ := range newShards {
		go kv.fetchShardFromGroup(shard, shardChan, stopChan)
	}

	paxosEvent := make(chan Op)
	stopPaxosWatcher := make(chan bool)

	go kv.paxosWatcher(paxosEvent, stopPaxosWatcher)

	var newShardOps []interface{}

	allShardsOp := make(chan Op, 1)

	addSentShardToPaxos := make(chan Op, 1)

	opAddedToPaxos := false

	for !kv.isdead() {
		select {
		//Got shard
		case operation := <-allShardsOp:
			kv.px.Start(kv.nextSeq, operation)
			val, _ := kv.waitForPaxos(kv.nextSeq)

			//TODO: Check err
			if val.(Op).ClientSeq == operation.ClientSeq {
				kv.Logf("New shards added to paxos")
				opAddedToPaxos = true
				if allTrue(oldShards) {
					kv.Logf("Config successfully applied after I received the last shard")
					kv.nextSeq++
					close(stopChan)
					close(stopPaxosWatcher)
					return
				}
			} else {
				kv.Logf("Shoot, not added to paxos!ClientSeq: %d, random: %d, val.op:%s", val.(Op).ClientSeq, operation.ClientSeq, val.(Op))
				time.Sleep(100 * time.Millisecond) //Get some time for paxoswatcher to catch up
				allShardsOp <- operation           //Put it back in!

			}

		case operation := <-addSentShardToPaxos:
			kv.px.Start(kv.nextSeq, operation)
			val, _ := kv.waitForPaxos(kv.nextSeq)

			//TODO: Check err
			if val.(Op).ClientSeq == operation.ClientSeq {
				kv.Logf("New shards added to paxos")
				oldShards[operation.Shard] = true
				if allTrue(oldShards) && opAddedToPaxos {
					kv.Logf("Config successfully applied after I received the last shard")
					kv.nextSeq++
				}
			} else {
				kv.Logf("Shoot, not added to paxos!ClientSeq: %d, random: %d", val.(Op).ClientSeq, operation.ClientSeq)
				time.Sleep(100 * time.Millisecond) //Get some time for paxoswatcher to catch up
				addSentShardToPaxos <- operation   //Put it back in!

			}

		case gotShard := <-kv.gotShardChan:
			kv.Logf("Got notification that shard %d was sent!", gotShard)
			random := rand.Int()
			newShardsOp := Op{Type: ShardSent, Shard: gotShard, Client: kv.me, ClientSeq: random}

			//Add to paxos:
			addSentShardToPaxos <- newShardsOp

		case response := <-shardChan:
			kv.Logf("Got a shard response!! :D")
			newShards[response.ShardNum] = true
			newShardOps = append(newShardOps, response.Shard...)

			if allTrue(newShards) {
				kv.Logf("Got all them shards, adding to paxos")
				random := rand.Int()
				newShardsOp := Op{Type: ShardsReceived, ShardOps: newShardOps, Client: kv.me, ClientSeq: random}

				//Add to paxos:
				allShardsOp <- newShardsOp

				kv.applyNewShards(newShardsOp)
			}

		case paxosOp := <-paxosEvent:
			if paxosOp.Type == NewConfig {
				//NewConfig -- We have a new leader! Cancel all goroutines and become follower
				kv.Logf("Somebody else took my leader role :( ")
				close(stopPaxosWatcher)
				close(stopChan)
				kv.waitForLeaderToGetThemShards(oldShards)
				return
			} else if paxosOp.Type == ShardSent {
				//ShardSent - We have succesfully sent a shard.
				kv.Logf("Yey, we sent a shard! ")
				oldShards[paxosOp.Shard] = true
				if opAddedToPaxos && allTrue(oldShards) {
					kv.Logf("Config successfully applied after I sent the last shard")
					//Config successfully applied!!
					close(stopPaxosWatcher)
					close(stopChan)
					return
				}
			}

			//Exit when all shards are sent and all shards received
		}
	}
}
func (kv *ShardKV) addToPaxosDuringReconfig(op Op) {
	for !kv.isdead() {
		//TODO: Duplicate detection, yes, no?

		kv.px.Start(kv.nextSeq, op)
		val, err := kv.waitForPaxos(kv.nextSeq)

		if err != nil {
			kv.Logf("ERRRROROOROROO!!!")
			continue
		}

		kv.applyOp(val.(Op))

		kv.nextSeq++

		//Did work?
		if val.(Op).Client == op.Client && val.(Op).ClientSeq == op.ClientSeq {
			kv.Logf("Applied operation in log at seq %d", kv.nextSeq-1)
			return
		} else {
			kv.Logf("Somebody else took seq %d before us, applying it and trying again", kv.nextSeq-1)
		}
	}
	return
}

func (kv *ShardKV) waitForLeaderToGetThemShards(oldShards map[int]bool) {
	//Called by all servers in a group but the leader upon a new config.
	kv.Logf("Wait for leader to get them shards")
	defer kv.Logf("No longer waiting for leader to get them shards")
	paxosEvent := make(chan Op)
	paxosWatchStop := make(chan bool)
	go kv.paxosWatcher(paxosEvent, paxosWatchStop)

	receivedShards := false

	for !kv.isdead() {

		select {

		case paxosOp := <-paxosEvent:
			if paxosOp.Type == ShardSent {
				//ShardSent - We have succesfully sent a shard.
				kv.Logf("ShardSent found in paxos log!")
				oldShards[paxosOp.Shard] = true
				if receivedShards && allTrue(oldShards) {
					//Config successfully applied!!
					kv.Logf("Config applied successfully")
					return
				}
			} else if paxosOp.Type == ShardsReceived {
				//New shards from leader: Apply shards
				kv.Logf("ShardReceived found in paxos log!")
				kv.applyNewShards(paxosOp)
				receivedShards = true

				if allTrue(oldShards) {
					//Config applied successfully
					kv.Logf("Config applied successfully")
					close(paxosWatchStop)
					return
				}
			}

		case <-time.After(500 * time.Millisecond):
			//TODO: Timeout: Try to become the next leader
		}
	}

}

func (kv *ShardKV) fetchShardFromGroup(shard int, replyChan chan shardResponse, stopChan chan bool) {
	kv.Logf("fetchShardFromGroup(shard: %d)", shard)
	args := &GetShardArgs{}
	args.Shard = shard
	kv.mu.Lock()
	args.ConfigNr = kv.currentConfig.Num
	gid := kv.oldConfig.Shards[shard]
	kv.mu.Unlock()
	var reply GetShardReply
	for {
		kv.mu.Lock()
		servers, ok := kv.oldConfig.Groups[gid]
		kv.mu.Unlock()
		if !ok {
			kv.Logf("fetchShardFromGroup NOT OK!! gid:%d ", gid, servers, kv.oldConfig)
			return
		}
		// try each server in the shard's replication group.
		for _, srv := range servers {
			kv.Logf("Calling GetShard!")
			ok := call(srv, "ShardKV.GetShard", args, &reply)
			kv.Logf("GetShard returned. Ok: %b!", ok)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				kv.Logf("Got shard %d from gid %d!", shard, gid)
				replyChan <- shardResponse{reply.Ops, shard}
				kv.sendGotShard(shard, gid)
				return
			}
			if ok && (reply.Err == ErrWrongGroup) {
				kv.Logf("Got 'ErrWrongGroup' for shard %d from gid %d!", shard, gid)
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
	return
}

func (kv *ShardKV) sendGotShard(shard int, gid int64) {

	kv.Logf("sendGotShard()")
	args := &GotShardArgs{}
	args.Shard = shard

	kv.mu.Lock()
	args.ConfigNr = kv.currentConfig.Num
	servers := kv.oldConfig.Groups[gid]
	kv.mu.Unlock()
	var reply GotShardReply
	for {

		// try each server in the shard's replication group.
		for _, srv := range servers {
			kv.Logf("Calling GotShard!")
			ok := call(srv, "ShardKV.GotShard", args, &reply)
			kv.Logf("GotShard returned. Ok: %b!", ok)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				kv.Logf("Got shard %d from gid %d!", shard, gid)
				return
			}
			if ok && (reply.Err == ErrWrongGroup) {
				kv.Logf("Got 'ErrWrongGroup' for shard %d from gid %d!", shard, gid)
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
	return
}

func (kv *ShardKV) applyNewShards(newShardOp Op) {
	kv.Logf("applyNewShards()")
	ops := newShardOp.ShardOps
	for _, op := range ops {
		kv.Logf("Applying shard operation: %o", op)
		kv.applyOp(op.(Op))
	}
}

func (kv *ShardKV) paxosWatcher(opChan chan Op, stop chan bool) {
	kv.Logf("Paxoswatcher started")
	defer kv.Logf("Paxoswatcher stopped")
	for !kv.isdead() {
		select {
		case <-stop:
			return
		case <-time.After(20 * time.Millisecond):
			for kv.px.Max() >= kv.nextSeq {
				kv.Logf("paxosWatcher found new paxos entry! %d", kv.nextSeq)
				val, err := kv.waitForPaxos(kv.nextSeq)
				if err != nil {
					log.Fatal("paxosWatcher ERROR!!! ", err)
				}
				opChan <- val.(Op)
				kv.nextSeq++
			}
		}
	}
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	//TODO: Freeze everything when responding? Yes? OMG, can I deadlock?!

	kv.Logf("GetShard()")

	if kv.currentConfig.Num != args.ConfigNr {
		kv.Logf("GetShard() return wrong group! currentNum %d, argnum: %d", kv.currentConfig.Num, args.ConfigNr)
		reply.Err = ErrWrongGroup
		//return nil
	}

	reply.Ops = make([]interface{}, 0)

	for key, val := range kv.database {
		if key2shard(key) == args.Shard {
			reply.Ops = append(reply.Ops, Op{Type: Put, Key: key, Value: val})
		}
	}

	reply.Err = OK
	return nil
}

func (kv *ShardKV) GotShard(args *GotShardArgs, reply *GotShardReply) error {
	//TODO: Freeze everything when responding? Yes? OMG, can I deadlock?!

	kv.Logf("GotShard()")

	//TODO: Add to paxos log

	//kv.gotShardChan <- args.Shard

	reply.Err = OK
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.Logf("Server Get(%s)! Shard: %d", args.Key, key2shard(args.Key))

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
	kv.Logf("Tick()")
	newConfig := kv.sm.Query(-1)
	if newConfig.Num == -1 || newConfig.Num == kv.currentConfig.Num {
		return
	}

	newConfig = kv.sm.Query(kv.nextConfigNum)

	kv.Logf("Tick(): New configuration!")
	op := Op{Type: NewConfig, Config: newConfig, Client: kv.me, ClientSeq: newConfig.Num}

	opReq := OpReq{op, make(chan string, 1), make(chan Err, 1)}

	kv.opReqChan <- opReq

	kv.nextConfigNum++

}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
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
	kv.gotShardChan = make(chan int)
	kv.myShard = make(map[int]bool)

	kv.oldConfig = shardmaster.Config{Num: -1}
	kv.currentConfig = shardmaster.Config{Num: -1}

	kv.nextConfigNum = 1

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
