package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"
import "errors"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
    view       *viewservice.View
    primary     string
    backup      string
    lock    *sync.Mutex
    data        map[string]string
    executed    map[int64]bool      //executed[uid] is true if the command with said uid has been executed
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    if(pb.primary != pb.me ){
        reply.Err = ErrWrongServer
        return errors.New(string(reply.Err))
    }

    pb.lock.Lock()
    defer pb.lock.Unlock()


    for pb.primary == pb.me &&
        pb.backup != "" &&
        !call(pb.backup, "PBServer.GetBackup", args, &reply){
            fmt.Printf("Error relaying to backup, rechecking viewserver\n")
            time.Sleep(viewservice.PingInterval)
            pb.tick()  //TODO: This seems ugly
    }

    reply.Value = pb.data[args.Key]
    fmt.Printf("Get(%s)=%s\n", args.Key, reply.Value)

	return nil
}

func (pb *PBServer) GetBackup(args *GetArgs, reply *GetReply) error {
    if(pb.backup != pb.me ){
        reply.Err = ErrWrongServer
        return errors.New(string(reply.Err))
    }
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    pb.lock.Lock()
    defer pb.lock.Unlock()

    if(pb.primary != pb.me ){
        reply.Err = ErrWrongServer
        fmt.Printf("Wrong server for %s(%s)=%s -- primary\n", args.Op, args.Key, args.Value)
        pb.tick()  //TODO: This seems ugly
        return errors.New(string(reply.Err))
    }

    if(pb.executed[args.UID]){
        fmt.Printf("ALREADY EXECUTED %s(%s)=%s -- primary\n", args.Op, args.Key, args.Value)
        //No error, just return normally as the request has been handled
        reply.Err = OK
        return  nil
    }

    for pb.primary == pb.me &&
        pb.backup != "" &&
        !call(pb.backup, "PBServer.PutAppendBackup", args, &reply){
            fmt.Printf("Error relaying to backup, rechecking viewserver\n")
            time.Sleep(viewservice.PingInterval)
            pb.tick()  //TODO: This seems ugly
    }

    if args.Op == "Put" {
        pb.data[args.Key] = args.Value
    }else if args.Op == "Append" {
        pb.data[args.Key] = pb.data[args.Key] + args.Value
    }else{
        fmt.Printf("Malformed PutAppend operation: %s\n", args.Op)
    }

    pb.executed[args.UID] = true
    fmt.Printf("Primary executed %s(%s)=%s\n", args.Op, args.Key, args.Value)
    reply.Err = OK
	return nil
}


func (pb *PBServer) PutAppendBackup(args *PutAppendArgs, reply *PutAppendReply) error {
    if(pb.backup != pb.me ){
        reply.Err = ErrWrongServer
        fmt.Printf("Wrong server for %s(%s)=%s -- backup\n", args.Op, args.Key, args.Value)
        pb.tick()  //TODO: This seems ugly
        return errors.New(string(reply.Err))
    }

    //Synchronize the rest of this function
    pb.lock.Lock()
    defer pb.lock.Unlock()


    if(pb.executed[args.UID]){
        //No error, just return normally as the request has been handled
        fmt.Printf("ALREADY EXECUTED %s(%s)=%s -- backup\n", args.Op, args.Key, args.Value)
        reply.Err = OK
        return nil
    }

    if args.Op == "Put" {
        pb.data[args.Key] = args.Value

    }else if args.Op == "Append" {
        pb.data[args.Key] = pb.data[args.Key] + args.Value

    }else{
        fmt.Printf("Malformed PutAppend operation: %s\n", args.Op)
    }

    pb.executed[args.UID] = true
    reply.Err = OK
    fmt.Printf("Backup executed %s(%s)=%s\n", args.Op, args.Key, args.Value)
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
    var err error
    *pb.view, err = pb.vs.Ping(pb.view.Viewnum)
    if(err != nil){
        fmt.Println("Tick error: %s", err)
    }

    if(pb.view.Primary == pb.me && pb.backup != pb.view.Backup && pb.view.Backup != ""){
        //New Backup
        pb.transferDataToBackup()
    }


    pb.primary = pb.view.Primary
    pb.backup = pb.view.Backup
}

func (pb *PBServer) transferDataToBackup(){
    fmt.Println("NEW BACKUP! Transfering data")
    reply := TransferDataReply{}
    args := TransferDataArgs{Data:pb.data, Executed: pb.executed}
    call(pb.view.Backup, "PBServer.ReceiveDatabase", args, &reply)
}

func (pb *PBServer) ReceiveDatabase(args *TransferDataArgs, reply *TransferDataReply) error {
    pb.data = args.Data
    pb.executed = args.Executed
    reply.Err = OK
    fmt.Println("DATA RECEIVED")
    return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
    pb.view = &viewservice.View{}
    pb.lock = &sync.Mutex{}
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
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
