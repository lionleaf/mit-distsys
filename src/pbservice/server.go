package pbservice

import "net"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "fmt"
import "syscall"
import "math/rand"
import "errors"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
    view       *viewservice.View
    primary     string
    backup      string
    requestlock *sync.Mutex //A lock to make sure we don't have multiple requests doing stuff at the same time
    ticklock    *sync.Mutex //A lock to make sure we don't process requests during backup setup
    data        map[string]string
    executed    map[int64]bool      //executed[uid] is true if the command with said uid has been executed
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    if(pb.primary != pb.me ){
        reply.Err = ErrWrongServer
        return errors.New(string(reply.Err))
    }

    pb.requestlock.Lock()
    defer pb.requestlock.Unlock()

    pb.ticklock.Lock()
    defer pb.ticklock.Unlock()

    for pb.primary == pb.me &&
        pb.backup != "" &&
        !call(pb.backup, "PBServer.GetBackup", args, &reply){
            DebugPrintf("Error relaying to backup, rechecking viewserver\n")
            pb.ticklock.Unlock()
            time.Sleep(viewservice.PingInterval)
            pb.ticklock.Lock()
    }

    reply.Value = pb.data[args.Key]
    DebugPrintf("Get(%s)=%s\n", args.Key, reply.Value)

	return nil
}

//RPC for a Get request to the backup
func (pb *PBServer) GetBackup(args *GetArgs, reply *GetReply) error {
    if(pb.backup != pb.me ){
        reply.Err = ErrWrongServer
        return errors.New(string(reply.Err))
    }
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    pb.requestlock.Lock()
    pb.ticklock.Lock()
    defer pb.requestlock.Unlock()
    defer pb.ticklock.Unlock()

    if(pb.primary != pb.me ){
        reply.Err = ErrWrongServer
        DebugPrintf("Wrong server for %s(%s)=%s -- primary\n", args.Op, args.Key, args.Value)
        return errors.New(string(reply.Err))
    }


    for pb.primary == pb.me &&
        pb.backup != "" &&
        !call(pb.backup, "PBServer.PutAppendBackup", args, &reply){
            DebugPrintf("Error relaying to backup, rechecking viewserver\n")
            pb.ticklock.Unlock()
            time.Sleep(viewservice.PingInterval)
            pb.ticklock.Lock()
    }

    if(pb.executed[args.UID]){
        DebugPrintf("ALREADY EXECUTED %s(%s)=%s -- primary\n", args.Op, args.Key, args.Value)
        //No error, just return normally as the request has been handled
        reply.Err = OK
        return  nil
    }

    if args.Op == "Put" {
        pb.data[args.Key] = args.Value
    }else if args.Op == "Append" {
        pb.data[args.Key] = pb.data[args.Key] + args.Value
    }else{
        DebugPrintf("Malformed PutAppend operation: %s\n", args.Op)
    }

    pb.executed[args.UID] = true
    DebugPrintf("Primary executed %s(%s)=%s\n", args.Op, args.Key, args.Value)
    reply.Err = OK
	return nil
}


//RPC function for the Primary calls on the Backup to make it replicate a Put/Append
func (pb *PBServer) PutAppendBackup(args *PutAppendArgs, reply *PutAppendReply) error {
    //Synchronize this function
    pb.requestlock.Lock()
    defer pb.requestlock.Unlock()

    if(pb.backup != pb.me ){
        reply.Err = ErrWrongServer
        DebugPrintf("Wrong server for %s(%s)=%s -- backup\n", args.Op, args.Key, args.Value)
        return errors.New(string(reply.Err))
    }



    if(pb.executed[args.UID]){
        //No error, just return normally as the request has been handled
        DebugPrintf("ALREADY EXECUTED %s(%s)=%s -- backup\n", args.Op, args.Key, args.Value)
        reply.Err = OK
        return nil
    }

    if args.Op == "Put" {
        pb.data[args.Key] = args.Value

    }else if args.Op == "Append" {
        pb.data[args.Key] = pb.data[args.Key] + args.Value

    }else{
        DebugPrintf("Malformed PutAppend operation: %s\n", args.Op)
    }

    pb.executed[args.UID] = true
    reply.Err = OK
    DebugPrintf("Backup executed %s(%s)=%s\n", args.Op, args.Key, args.Value)
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
    //As tick is called from within locked code we need a separate lock to avoid deadlock
    pb.ticklock.Lock()
    defer pb.ticklock.Unlock()

    var err error
    *pb.view, err = pb.vs.Ping(pb.view.Viewnum)
    if(err != nil){
        DebugPrintf("Tick error: %s\n", err)
    }

    if(pb.view.Primary == pb.me && pb.backup != pb.view.Backup && pb.view.Backup != ""){
        //New Backup
        pb.transferDataToBackup()
    }


    pb.primary = pb.view.Primary
    pb.backup = pb.view.Backup
}

func (pb *PBServer) transferDataToBackup(){
    DebugPrintf("NEW BACKUP! Transfering data\n")
    reply := TransferDataReply{}
    args := TransferDataArgs{Data:pb.data, Executed: pb.executed}
    call(pb.view.Backup, "PBServer.ReceiveDatabase", args, &reply)
}

func (pb *PBServer) ReceiveDatabase(args *TransferDataArgs, reply *TransferDataReply) error {
    //Synchronize this function
    pb.ticklock.Lock()
    defer pb.ticklock.Unlock()

    pb.data = args.Data
    pb.executed = args.Executed
    reply.Err = OK
    DebugPrintf("DATA RECEIVED\n")
    return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
    pb.view = &viewservice.View{}
    pb.requestlock = &sync.Mutex{}
    pb.ticklock = &sync.Mutex{}
    pb.data = make(map[string]string)
    pb.executed = make(map[int64]bool)
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						DebugPrintf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
