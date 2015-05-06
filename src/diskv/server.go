package diskv

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
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"

const Debug = 1

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
	Shard     int
	ShardOps  []interface{}
	Client    int
	ClientSeq int
	Config    shardmaster.Config
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

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

var log_mu sync.Mutex

func (kv *DisKV) Logf(format string, a ...interface{}) {
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

// For config OPs, set ClientSeq = Config.Num and Client = 0

type OpReq struct {
	op        Op
	replyChan chan string
	errChan   chan Err
}

func (kv *DisKV) sequentialApplier() {
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
func (kv *DisKV) ping() {
	dummyOp := Op{Type: Get}
	for !kv.isdead() {
		fate, val := kv.px.Status(kv.nextSeq)

		if fate == paxos.Decided {
			kv.nextSeq++
			kv.dumpState()
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

func (kv *DisKV) addToPaxos(op Op) {
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
		kv.dumpState()
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
func (kv *DisKV) applyOp(op Op) {
	//Note, don't update clientseq outside conditionals,
	//as there is no guarantee Gets can't be out of order
	if op.Type == Put {
		kv.clientSeqs[op.Client] = op.ClientSeq
		kv.Logf("Applying put(%s) to database", op.Key)
		kv.database[op.Key] = op.Value
		kv.writeKeyToDisk(op.Key)
	} else if op.Type == Append {
		kv.clientSeqs[op.Client] = op.ClientSeq
		kv.Logf("Applying append(%s) to database", op.Key)
		kv.database[op.Key] = kv.database[op.Key] + op.Value
		kv.writeKeyToDisk(op.Key)
	} else if op.Type == NewConfig {
		kv.applyNewConfig(op.Config, op.Client == kv.me)
	}

	//Do nothing for get
}

//Handle a new config and freeze(don't return) until we added all new keys
func (kv *DisKV) applyNewConfig(newConfig shardmaster.Config, leader bool) {
	if newConfig.Num == kv.currentConfig.Num {
		kv.Logf("Duplicate changeconfig in paxos log")
		return
	}

	kv.Logf("New config! Freeze until everything is sorted")

	newShards := make(map[int]bool) //New shards I got responsibility for
	oldShards := make(map[int]bool) //Shards that I'm no longer responsible for, make sure they get transfered

	for shard, gid := range newConfig.Shards {
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

	//TODO: Should you wait until you are sure all the shards have been taking by the other group?
	if len(newShards) == 0 /*&& len(oldShards) == 0 */ {
		kv.Logf("No shards to get or send!")
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

func (kv *DisKV) newConfigLeader(newShards map[int]bool, oldShards map[int]bool) {
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
					kv.dumpState()
					close(stopChan)
					close(stopPaxosWatcher)
					return
				}
			} else {
				kv.Logf("Shoot, not added to paxos!ClientSeq: %d, random: %d, val.op:%s", val.(Op).ClientSeq, operation.ClientSeq, val.(Op))
				time.Sleep(100 * time.Millisecond) //Get some time for paxoswatcher to catch up
				allShardsOp <- operation           //Put it back in!

			}

		//Sent a shard, add it to paxos!
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
					kv.dumpState()
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
func (kv *DisKV) addToPaxosDuringReconfig(op Op) {
	for !kv.isdead() {
		//TODO: Duplicate detection, yes, no?

		kv.px.Start(kv.nextSeq, op)
		val, err := kv.waitForPaxos(kv.nextSeq)

		if err != nil {
			kv.Logf("ERRRROROOROROO!!!")
			continue
		}

		kv.applyOp(val.(Op))

		//TODO: What happens if crash between these two lines?
		kv.nextSeq++
		kv.dumpState()

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

func (kv *DisKV) waitForLeaderToGetThemShards(oldShards map[int]bool) {
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

func (kv *DisKV) fetchShardFromGroup(shard int, replyChan chan shardResponse, stopChan chan bool) {
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
			ok := call(srv, "DisKV.GetShard", args, &reply)
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

func (kv *DisKV) sendGotShard(shard int, gid int64) {
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
			ok := call(srv, "DisKV.GotShard", args, &reply)
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

func (kv *DisKV) applyNewShards(newShardOp Op) {
	kv.Logf("applyNewShards()")
	ops := newShardOp.ShardOps
	for _, op := range ops {
		kv.Logf("Applying shard operation: %o", op)
		kv.applyOp(op.(Op))
	}
}

func (kv *DisKV) paxosWatcher(opChan chan Op, stop chan bool) {
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
				kv.dumpState()
			}
		}
	}
}

func (kv *DisKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
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

func (kv *DisKV) GotShard(args *GotShardArgs, reply *GotShardReply) error {
	//TODO: Freeze everything when responding? Yes? OMG, can I deadlock?!

	kv.Logf("GotShard()")

	//TODO: Add to paxos log

	//kv.gotShardChan <- args.Shard

	reply.Err = OK
	return nil
}

func (kv *DisKV) writeKeyToDisk(key string) {
	kv.Logf("Dumping key %s to disk!", key)
	kv.filePut(key2shard(key), kv.encodeKey(key), kv.database[key])
}

/**
 * Dumps important state to disk
**/
func (kv *DisKV) dumpState() error {
	fullname := kv.stateDir() + "/seq"
	tempname := kv.stateDir() + "/temp"
	content := string(kv.nextSeq)

	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

//Directory to hold various important state
func (kv *DisKV) stateDir() string {
	d := kv.dir + "/state/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) recoverFromDisk() {
	kv.recoverDatabaseFromDisk()
	kv.recoverSeq()
}

func (kv *DisKV) recoverSeq() {
	fullname := kv.stateDir() + "seq"
	content, _ := ioutil.ReadFile(fullname)

	//TODO: Convert more elegantly
	recSeq, _ := strconv.ParseInt(string(content), 10, 32)
	kv.nextSeq = int(recSeq)

}

func (kv *DisKV) recoverDatabaseFromDisk() {
	//TODO: FIXME!!! Hardcoded for debugging
	n_shards := 9
	for i := 0; i < n_shards; i++ {
		shard := kv.fileReadShard(i)
		for k, v := range shard {
			kv.database[k] = v
		}
	}
}
func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
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
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.Logf("Server PutAppend!")

	op := Op{Type: args.Op, Key: args.Key,
		Value: args.Value, Client: args.Client, ClientSeq: args.ClientSeq}

	opReq := OpReq{op, make(chan string, 1), make(chan Err, 1)}

	kv.opReqChan <- opReq

	reply.Err = <-opReq.errChan

	return nil
}

func (kv *DisKV) waitForPaxos(seq int) (val interface{}, err error) {
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
func (kv *DisKV) tick() {
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
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	kv.Logf("Storage directory: %s ", dir)

	// Your initialization code here.
	// Don't call Join().
	kv.database = make(map[string]string)
	kv.clientSeqs = make(map[int]int)
	kv.opReqChan = make(chan OpReq)
	kv.gotShardChan = make(chan int)
	kv.myShard = make(map[int]bool)

	kv.oldConfig = shardmaster.Config{Num: -1}
	kv.currentConfig = shardmaster.Config{Num: -1}

	kv.nextConfigNum = 1

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	if restart {
		kv.recoverFromDisk()
	}

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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
