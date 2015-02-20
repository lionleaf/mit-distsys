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



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
    view       *viewservice.View
    primary     bool
    backup      bool
    data        map[string]string
	// Your declarations here.
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    reply.Value = pb.data[args.Key]
    fmt.Printf("Get(%s)=%s\n", args.Key, reply.Value)
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    if args.Op == "Put"{
        pb.data[args.Key] = args.Value
    }else if args.Op == "Append" {
        pb.data[args.Key] = pb.data[args.Key] + args.Value
    }else{
        fmt.Printf("Malformed PutAppend operation: %s\n", args.Op)
    }

    fmt.Printf("PutAppend(%s, %s,%s)\n", args.Key, args.Value, args.Op)
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

    if pb.view.Primary == pb.me {
        pb.primary = true
        pb.backup = false
    }else if pb.view.Backup == pb.me{
        pb.primary = false
        pb.backup = true
    }else{
        pb.primary = false
        pb.backup = false
    }
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
    pb.data = make(map[string]string)
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
