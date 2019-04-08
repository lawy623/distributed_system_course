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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "math"
import "time"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type Proposal struct{
  n_p int
  n_a int
  v_a interface{}
  status bool
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  history map[int]Proposal
  decisions map[int]int
  maxSeq int
  majorSize int
}

//self define reply type
const (
  PrepareOK = "PrepareOK"
  PrepareERR = "PrepareERR"
  AcceptOK = "AcceptOK"
  AcceptERR = "AcceptERR"
  DecideOK = "DecideOK"
)
type Err string

//Prepare RPC's
type PrepareArgs struct {
  Seq int
  N int
}
type PrepareReply struct {
  N_a int
  V_a interface{}
  Done int
  Err Err
}

//Accept RPC's
type AcceptArgs struct {
  Seq int
  N int
  V interface{}
}
type AcceptReply struct {
  N_a int
  Done int
  Err Err
}

//Decide RPC's
type DecideArgs struct {
  Seq int
  N_a int
  V_a interface{}
}
type DecideReply struct {
  Done int
  Err Err
}

func max (a int, b int) int {
  if a > b {
    return a
  } else {
    return b
  }
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

func (px *Paxos) PrintHis() {
  px.mu.Lock()
  defer px.mu.Unlock()

  his := px.history
  len := len(his)

  DPrintf("   Current len of paxos history instance of paxos server %d is %d...\n", px.me, len)
  for k, v := range his {
    DPrintf("   [Instance %d | Status %t]\n", k, v.status)
  }
  DPrintf("\n")
}

// Find the proposal detail in history log.
// Help to know whether this proposal has been decided or not.
func (px *Paxos) getProp(seq int) Proposal {
  px.mu.Lock()
  defer px.mu.Unlock()
  //DPrintf("Finding Seq in History...\n")

  _, ok := px.history[seq]
  if ok {
    //DPrintf("   Find Seq %v in Paxos %v..\n", seq, px.me)
    px.maxSeq = max(seq, px.maxSeq)
  } else {
    //DPrintf("   !!Not Find Seq %v in Paxos %v..\n", seq, px.me)
    px.history[seq] = Proposal{-1, -1, nil, false}
  }

  return px.history[seq]
}

func (px *Paxos) GetHis() map[int]bool {
  px.mu.Lock()
  defer px.mu.Unlock()

  res := make(map[int]bool)
  for k,v := range px.history {
    res[k] = v.status
  }
  return res
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
  go func(seq int, v interface{}) {
    DPrintf("&&&&&&&&&&&&&&&  Paxos %v making proposal [%d]..\n", px.me, seq)
    n := 0
    ins := px.getProp(seq)

    for px.dead==false && ins.status==false { //If the proposal has not been decided yet.
      DPrintf("Making for Seq %v..\n", seq)
      DPrintf("Current Status [np: %v, na: %v, nv: %s, status: %v]...\n", ins.n_p, ins.n_a, ins.v_a, ins.status)
      count := 0
      max_n := -1
      max_v := v

      //Prepare stage.
      DPrintf("$$$$$$$  Prepare Stage $$$$$$$$$\n")
      for i := 0; i < len(px.peers); i++ {
        args := &PrepareArgs{seq, n}
        var reply PrepareReply
        if px.sendPrepare(i, args, &reply) && reply.Err == PrepareOK {
          DPrintf("Receive Agreement from %v..\n", i)
          px.mu.Lock()
          px.decisions[i] = reply.Done
          px.mu.Unlock()
          count = count + 1
          if reply.N_a > max_n {
            max_n = reply.N_a
            max_v = reply.V_a
          }
        } else {
          DPrintf("No Agreement from %v..\n", i)
        }
      }
      if count < px.majorSize { // do not make majority prepare
        ins = px.getProp(seq)
        n = max(max_n, n) + 1
        DPrintf("No Majority Prepare. Change n to [%v]..\n", n)
        time.Sleep(20 * time.Millisecond)
        continue
      }

      // Accept stage. Will come here if majority are prepared.
      DPrintf("$$$$$$$  Accept Stage $$$$$$$$$\n")
      count = 0
      for i := 0; i < len(px.peers); i++ {
        args := &AcceptArgs{seq, n, max_v}
        var reply AcceptReply
        if px.sendAccept(i, args, &reply) && reply.Err == AcceptOK {
          px.mu.Lock()
          px.decisions[i] = reply.Done
          px.mu.Unlock()
          count = count + 1
        }
      }
      if count < px.majorSize { // do not make majority Accept
        ins = px.getProp(seq)
        n = max(max_n, n) + 1
        time.Sleep(20 * time.Millisecond)
        continue
      }

      // Decide stage
      DPrintf("$$$$$$$  Decide Stage $$$$$$$$$\n")
      for i := 0; i < len(px.peers); i++ {
        args := &DecideArgs{seq, n, max_v}
        var reply DecideReply
        if px.sendDecide(i, args, &reply) && reply.Err == DecideOK {
          px.mu.Lock()
          px.decisions[i] = reply.Done
          px.mu.Unlock()
        }
      }
      DPrintf("After a new Instance %d, history table becomes...\n", seq)
      px.PrintHis();
      break
    }
  }(seq, v)
}

func (px *Paxos) sendPrepare(index int, args *PrepareArgs, reply *PrepareReply) bool {
  if index == px.me {
    return px.Prepare(args, reply) == nil
  } else {
    return call(px.peers[index], "Paxos.Prepare", args, reply)
  }
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  DPrintf("Paxos %v prepare to vote...\n", px.me)
  _, ok := px.history[args.Seq]
  if !ok {
    px.history[args.Seq] = Proposal{-1, -1, nil, false}
    px.maxSeq = max(px.maxSeq, args.Seq)
  }
  ins := px.history[args.Seq]
  reply.Done = px.decisions[px.me]

  if args.N > ins.n_p {
    DPrintf("Paxos %v agree to Vote...\n", px.me)
    px.history[args.Seq] = Proposal{args.N, ins.n_a, ins.v_a, ins.status}
    reply.Err = PrepareOK
    reply.N_a = px.history[args.Seq].n_a
    reply.V_a = px.history[args.Seq].v_a
  } else {
    DPrintf("Paxos %v does not Agree...\n", px.me)
    DPrintf("Present Value is [%v], ask value is [%v]...\n", ins.n_p, args.N)
    reply.Err = PrepareERR
  }
  return nil
}

func (px *Paxos) sendAccept(index int, args *AcceptArgs, reply *AcceptReply) bool {
  if index == px.me {
    return px.Accept(args, reply) == nil
  } else {
    return call(px.peers[index], "Paxos.Accept", args, reply)
  }
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.history[args.Seq] //How if this does not exist
  reply.Done = px.decisions[px.me]

  if args.N >= ins.n_p {
    DPrintf("Paxos %v agree to Accept...\n", px.me)
    px.history[args.Seq] = Proposal{args.N, args.N, args.V, ins.status}
    reply.Err = AcceptOK
    reply.N_a = args.N
  } else {
    DPrintf("Paxos %v does not Accept...\n", px.me)
    DPrintf("Present Value is [%v], ask value is [%v]...\n", ins.n_p, args.N)
    reply.Err = AcceptERR
  }
  return nil
}

func (px *Paxos) sendDecide(index int, args *DecideArgs, reply *DecideReply) bool {
  if index == px.me {
    return px.Decide(args, reply) == nil
  } else {
    return call(px.peers[index], "Paxos.Decide", args, reply)
  }
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  DPrintf("Paxos %v Decide...\n", px.me)
  px.history[args.Seq] = Proposal{args.N_a, args.N_a, args.V_a, true}
  reply.Err = DecideOK
  reply.Done = px.decisions[px.me]
  return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  px.decisions[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()

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
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  min_seq := math.MaxInt32
  for _, v := range px.decisions {
    if v < min_seq {
      min_seq = v
    }
  }

  //Delete unnecessary instances
  for seq, ins := range px.history {
    if seq <= min_seq && ins.status {
      delete(px.history, seq)
    }

  }
  return min_seq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  DPrintf("Paxos %v is getting status ...\n", px.me)
  if seq < px.Min() {
    DPrintf("Required Status less than min available...\n")
    return false, nil
  }
  ins := px.getProp(seq)
  DPrintf("Return status '%v' with value %v...\n", ins.status, ins.v_a)
  return ins.status, ins.v_a
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
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

  // Your initialization code here.
  px.history = make(map[int]Proposal)
  px.decisions = make(map[int]int)
  for i := 0; i < len(px.peers); i++ {
    px.decisions[i] = -1
  }
  px.maxSeq = -1
  px.majorSize = len(px.peers)/2 + 1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
