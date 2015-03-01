package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

const DEBUG = false

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type AcceptorInstance struct {
	instance_ID int
	n_prep      int
	n_accept    int
	val_accept  interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	n_peers    int
	majority   int
	maxSeq     int
	decided    map[int]bool

	mins      []int
	min       int
	globalMin int

	//Proposer data, organized in maps
	n_highest map[int]int
	acceptor  map[int]*AcceptorInstance
	locks     map[int]*sync.Mutex

	globalLock sync.Mutex

	//Map of the values
	val map[int]interface{}
}

type PrepareArgs struct {
	N      int
	Seq    int
	Min    int
	Peer_n int
}

type AcceptArgs struct {
	N      int
	V      interface{}
	Seq    int
	Min    int
	Peer_n int
}

type PrepAcceptRet struct {
	OK     bool
	N_a    int
	V_a    interface{}
	Min    int
	Peer_n int
}

type DecidedArgs struct {
	Value  interface{}
	Seq    int
	Min    int
	Peer_n int
}
type DecidedRet struct {
	OK     bool
	Min    int
	Peer_n int
}

var log_mu sync.Mutex

func (px *Paxos) Logf(format string, a ...interface{}) {
	if !DEBUG {
		return
	}

	log_mu.Lock()
	defer log_mu.Unlock()

	me := px.me

	fmt.Printf("\x1b[%dm", (me%6)+31)
	fmt.Printf("S#%d : ", me)
	fmt.Printf(format+"\n", a...)
	fmt.Printf("\x1b[0m")
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		px.Propose(v, seq)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.min = seq
	px.updateGlobalMin()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return px.globalMin + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.lock(seq)
	defer px.unlock(seq)
	if seq <= px.globalMin {
		return Forgotten, nil
	}
	if px.decided[seq] {
		return Decided, px.val[seq]
	}
	return Pending, nil
}

func rndAbove(n int) int {
	randomSpace := int32(1 << 30)
	value := n + int(rand.Int31n(randomSpace))
	return int(value)
}

func (px *Paxos) minFromPeer(newMin int, peer int) {
	px.Logf("minFromPeer(%d,%d)\n", newMin, peer)
	if newMin > px.mins[peer] {
		px.Logf("New min from %d: %d \n", peer, newMin)
		px.mins[peer] = newMin
		px.updateGlobalMin()
	}

}

func (px *Paxos) updateGlobalMin() {
	oldGlobalMin := px.globalMin
	for _, min := range px.mins {
		if min > px.globalMin {
			px.globalMin = min
		}
	}

	if oldGlobalMin < px.globalMin {
		px.Logf("New global min!\n")
		px.freeResources(oldGlobalMin)
	}
}

func (px *Paxos) freeResources(prevMin int) {
	for i := prevMin; i < px.globalMin; i++ {
		delete(px.val, i)
		delete(px.acceptor, i)
	}
}

func (px *Paxos) attemptRPCMajority(rpcname string, args interface{}) (majority bool, ok_responses []PrepAcceptRet, highest_n int) {
	px.Logf("RPCMajority(%s)\n", rpcname)
	//Make it buffered so we don't have goroutines blocked forever.
	//There will never be more than n_peers, so we are safe.
	done := make(chan bool, px.n_peers)

	high_n := 0

	var rpcMutex sync.Mutex

	n_responses := 0
	n_ok := 0

	// Keep all the OK responses
	ok_resp := make([]PrepAcceptRet, 0, px.n_peers)

	//Lock to sync the different RPC calls

	// Start a goroutine with an rpc to every peer
	for i := range px.peers {
		peer := px.peers[i]
		me := i == px.me
		go func() {
			ret := PrepAcceptRet{}

			//RPC ok response
			ok := false

			if me && rpcname == "Paxos.Accept" {
				ok = true
				px.Accept(args.(AcceptArgs), &ret)
			} else {

				//Retry RPC up to 3 times
				for i := 0; i < 3 && !ok; i++ {
					px.Logf("call(%s,%s)\n", peer, rpcname)
					ok = call(peer, rpcname, args, &ret)
					if !ok {
						px.Logf("Retrying call(%s,%s)\n", peer, rpcname)
					}
				}
			}

			rpcMutex.Lock()

			px.Logf("ret.N_a : %d\n ", ret.N_a)
			if ret.N_a > high_n {
				high_n = ret.N_a
				px.Logf("High_n : %d\n ", high_n)
			}

			px.minFromPeer(ret.Min, ret.Peer_n)

			n_responses++
			if ok && ret.OK {
				n_ok++
				ok_resp = append(ok_resp, ret)
				px.Logf("Ok response recorded\n")
			}

			//If we have a majority ok or declines we don't need to wait for more responses
			if n_ok >= px.majority || (n_responses-n_ok) >= px.majority || px.isdead() {
				px.Logf("We are returning!!\n")
				done <- true
			}
			if n_responses == px.n_peers {
				close(done)
			}

			rpcMutex.Unlock()
		}()
	}

	//Wait for enough responses
	<-done
	px.Logf("The wait is over!!\n")

	rpcMutex.Lock()
	cond := n_ok >= px.majority
	rpcMutex.Unlock()

	if cond {
		return true, ok_resp, high_n
	}
	return false, nil, high_n
}

func (px *Paxos) Propose(val interface{}, Seq int) {

	px.Logf("Propose(%d)\n", Seq)
	//Data race condition
	px.globalLock.Lock()
	if Seq > px.maxSeq {
		px.maxSeq = Seq
	}
	px.globalLock.Unlock()

	decided := false
	for !decided && !px.isdead() {
		n := rndAbove(px.n_highest[Seq])

		prepMajority, prepOKResponses, high_n := px.attemptRPCMajority("Paxos.Prepare", PrepareArgs{n, Seq, px.min, px.me})
		px.Logf("%d: Prepare has returned: high_n: %d! \n", Seq, high_n)

		//Make sure we update the highest seen n even in case of declines
		if !prepMajority {
			px.lock(Seq)
			if high_n > px.n_highest[Seq] {
				px.n_highest[Seq] = high_n
				px.Logf("New high_n %d\n", high_n)
			}
			px.unlock(Seq)
		}

		if prepMajority {
			px.Logf("%d: Got majority prepare! \n", Seq)

			//Find the value
			highest_n := 0
			var highest_val interface{}

			for _, response := range prepOKResponses {
				if response.N_a > highest_n {
					highest_n = response.N_a
					highest_val = response.V_a
				}
			}

			if highest_n <= 0 { //No other values, use own
				px.Logf("%d: Choosing our own value! \n", Seq)
				highest_val = val
				highest_n = n
			}

			px.lock(Seq)
			px.n_highest[Seq] = highest_n
			px.unlock(Seq)

			acceptArgs := AcceptArgs{N: n, V: highest_val, Seq: Seq}

			acceptMajority, _, _ := px.attemptRPCMajority("Paxos.Accept", acceptArgs)
			px.Logf("%d: Accept has returned! \n", Seq)

			if acceptMajority {
				px.Logf("%d Got majority accept! \n", Seq)

				//px.Decided(DecidedArgs{highest_val, Seq}, nil)
				for i := range px.peers {
					ret := DecidedRet{}
					if i == px.me {
						px.Logf("%d Calling own Decided! \n", Seq)
						px.Decided(DecidedArgs{highest_val, Seq, px.min, px.me}, &ret)
					} else {
						peer := px.peers[i]

						var minMutex sync.Mutex
						go func() {
							px.Logf("%d RPC Decided! \n", Seq)
							ok := call(peer, "Paxos.Decided", DecidedArgs{highest_val, Seq, px.min, px.me}, &ret)

							minMutex.Lock()
							if ok {
								px.minFromPeer(ret.Min, ret.Peer_n)
							}
							minMutex.Unlock()
						}()
					}
				}
			}
		}

		px.lock(Seq)
		decided = px.decided[Seq]
		px.unlock(Seq)
	}
}

var lockLock sync.Mutex

func (px *Paxos) lock(Seq int) {

	//Make sure only one lock is created
	lockLock.Lock()
	if px.locks[Seq] == nil {
		px.locks[Seq] = &sync.Mutex{}
	}
	lockLock.Unlock()

	px.locks[Seq].Lock()
}

func (px *Paxos) unlock(Seq int) {
	px.locks[Seq].Unlock()
}

func (px *Paxos) Decided(args DecidedArgs, ret *DecidedRet) (err error) {
	px.Logf("Decided(%d)\n", args.Seq)
	px.lock(args.Seq)
	defer px.unlock(args.Seq)

	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}

	if !px.decided[args.Seq] {
		px.Logf("%d: VALUE DECIDED! \n", args.Seq)

		px.val[args.Seq] = args.Value
		px.decided[args.Seq] = true
	}

	ret.OK = true
	ret.Min = px.min
	ret.Peer_n = px.me
	return nil
}

func (px *Paxos) Prepare(args PrepareArgs, ret *PrepAcceptRet) (err error) {
	px.Logf("Prepare(%d)\n", args.Seq)
	px.lock(args.Seq)
	defer px.unlock(args.Seq)

	//For memory management
	ret.Min = px.min
	ret.Peer_n = px.me

	px.minFromPeer(args.Min, args.Peer_n)

	px.globalLock.Lock()
	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}
	px.globalLock.Unlock()

	px.globalLock.Lock()
	ac, OK := px.acceptor[args.Seq]
	if !OK {
		ac = &AcceptorInstance{}
		px.acceptor[args.Seq] = ac
	}
	px.globalLock.Unlock()

	if args.N > ac.n_prep {
		ac.n_prep = args.N

		px.Logf("%d prepare_ok(N_a: %d) args.N: %d, ac.n_prep: %d \n", args.Seq, ac.n_accept, args.N, ac.n_prep)
		ret.OK = true
		ret.N_a = ac.n_accept
		ret.V_a = ac.val_accept
	} else {
		px.Logf("%d prepare_decline(N: %d  N_prep: %d)! \n", args.Seq, args.N, ac.n_prep)
		ret.N_a = ac.n_prep
		px.Logf("%d ret.N_a %d \n", args.Seq, ret.N_a)
		ret.OK = false
	}
	return nil
}

func (px *Paxos) Accept(args AcceptArgs, ret *PrepAcceptRet) (err error) {
	px.Logf("Accept(%d)\n", args.Seq)
	px.lock(args.Seq)
	defer px.unlock(args.Seq)

	px.minFromPeer(args.Min, args.Peer_n)

	px.globalLock.Lock()
	if args.Seq > px.maxSeq {
		px.maxSeq = args.Seq
	}
	px.globalLock.Unlock()

	ac, OK := px.acceptor[args.Seq]
	if !OK {
		ac = &AcceptorInstance{}
		px.acceptor[args.Seq] = ac
	}

	n := args.N
	v := args.V

	if n >= ac.n_prep {
		px.Logf("%d accept_ok(N: %d  N_prep: %d)! \n", args.Seq, n, ac.n_prep)
		ac.n_prep = n
		ac.n_accept = n
		ac.val_accept = v
		ret.OK = true
	} else {
		px.Logf("%d accept_decline(N: %d  N_prep: %d)! \n", args.Seq, n, ac.n_prep)
		ret.OK = false
	}

	return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.val = make(map[int]interface{})
	px.decided = make(map[int]bool)
	px.n_highest = make(map[int]int)
	px.acceptor = make(map[int]*AcceptorInstance)
	px.locks = make(map[int]*sync.Mutex)
	px.n_peers = len(peers)
	px.majority = (px.n_peers / 2) + px.n_peers%2
	px.mins = make([]int, px.n_peers)

	px.globalMin = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
