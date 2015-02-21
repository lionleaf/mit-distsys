package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

    currentView View
    nextView    View //Has to be nil when no new view is pending
    viewConfirmed bool
    updatedBackupView uint

    lastSeen map[string]time.Time

    extraServers []string
    extraServerMap map[string]int // >1 if it is in the slice, to avoid duplicates

	// Your declarations here.
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
    vs.lastSeen[args.Me] = time.Now()

    if vs.currentView.Viewnum == 0{ //First reporting serving becomes first Primary!
        vs.currentView.Viewnum = 1;
        vs.currentView.Primary = args.Me;
    } else if args.Me == vs.currentView.Primary && args.Viewnum == 0 {
        //Primary died and restarted!
        vs.promoteBackup()
        vs.freeServerPing(args.Me)
    } else if args.Me == vs.currentView.Backup && args.Viewnum == 0 && vs.updatedBackupView == vs.currentView.Viewnum  {
        //Backup died and restarted!
        vs.removeBackup()
        vs.freeServerPing(args.Me)
    }else if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum {
        vs.viewConfirmed = true
    }else if args.Me == vs.currentView.Backup && args.Viewnum == vs.currentView.Viewnum {
        vs.updatedBackupView = args.Viewnum
    } else if args.Me != vs.currentView.Backup && args.Me != vs.currentView.Primary {
        vs.freeServerPing(args.Me);
    }

    //vs.moveViewForward();

    reply.View = vs.currentView
	return nil
}

func (vs *ViewServer) freeServerPing(server string){
        if vs.extraServerMap[server] == 0{
            vs.extraServers = append(vs.extraServers, server) //Changing views only happen in tick()
            vs.extraServerMap[server] = 1
        }
}

func (vs *ViewServer) promoteBackup() {
    vs.nextView.Viewnum = vs.currentView.Viewnum + 1
    vs.nextView.Primary = vs.currentView.Backup
    vs.nextView.Backup  = ""
}

func (vs *ViewServer) removeBackup() {
    vs.nextView = View{Viewnum: vs.currentView.Viewnum + 1 ,
                    Primary: vs.currentView.Primary ,
                    Backup: ""}
}

func (vs *ViewServer) moveViewForward() {
    if vs.nextView.Viewnum > vs.currentView.Viewnum && vs.viewConfirmed {
        vs.currentView.Viewnum = vs.nextView.Viewnum;
        vs.currentView.Primary = vs.nextView.Primary;
        vs.currentView.Backup = vs.nextView.Backup;
        vs.viewConfirmed = false;
        vs.nextView.Viewnum = 0;
    }
}
//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
    reply.View = vs.currentView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
    view := &vs.currentView;

    if (view.Backup == "" || vs.timeout(view.Backup)) && len(vs.extraServers) > 0 {
        newBackup := pop(&vs.extraServers)
        vs.extraServerMap[newBackup] = 0

        vs.nextView = View{Viewnum: view.Viewnum + 1 ,
                        Primary: view.Primary ,
                        Backup: newBackup}

    } else if vs.timeout(view.Primary) {
        vs.promoteBackup()
    } else if vs.timeout(view.Backup) {
        vs.removeBackup()
    }

    vs.moveViewForward()
}

func pop(slice *[]string) string {
    var x string
    slc := *slice
    x, slc = slc[len(slc) - 1], slc[:len(slc) - 1]
    *slice = slc
    return x
}


func (vs *ViewServer) timeout(server string)bool{
    if time.Since(vs.lastSeen[server]) > (DeadPings * PingInterval){
        return true
    }
    return false
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
    vs.currentView = View{0,"",""}
    vs.lastSeen = make(map[string]time.Time)
    vs.extraServers = make([]string, 0)
    vs.extraServerMap = make(map[string]int)
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
