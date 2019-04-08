package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "reflect"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  seq int //Get to know the current seq
  maxConfig int //Record the maxConfig num of this server
}

const (
  JOIN = "Join"
  LEAVE = "Leave"
  MOVE = "Move"
  QUERY = "Query"
)

type Op struct {
  // Your data here.
  Operation string
  Args interface{}
}

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}


//Helper function to print the current configs in servers.
func (sm *ShardMaster) PrintConfig() {
  DPrintf("Printing Configs information...\n")
  DPrintf("   Current len of Config is [%d]...\n", sm.maxConfig+1)
  DPrintf("   Testing len of Config [%d==%d] => \"%t\"...\n", sm.maxConfig+1, len(sm.configs), sm.maxConfig+1==len(sm.configs))
  for k, v := range sm.configs {
    DPrintf("   [Index %d | Num %d] -->", k, v.Num)
    //
      DPrintf("(%d Shards) [", len(v.Shards))
      for a,_ := range v.Shards {
        DPrintf("%d ", v.Shards[a])
      }
      DPrintf("]->")
    //

    DPrintf("(%d Groups)", len(v.Groups))
    //for g, ser := range v.Groups {
    //  DPrintf(" Gid: %d-Ser %s |", g, ser)
    //}
    DPrintf("\n")
  }
  DPrintf("\n")
}

// Helper function to print paxos logs
func (sm *ShardMaster) PrintPaxosHis() {
  his := sm.px.GetHis()
  len := len(his)

  DPrintf("   Current len of paxos history instance of kv server %d is %d...\n", sm.me, len)
  for k, v := range his {
    DPrintf("   [Instance %d | Status %t]\n", k, v)
  }
  DPrintf("\n")
}

//Wait for the status
func (sm *ShardMaster) Wait(seq int) Op {
  DPrintf("Waiting for Seq %d to confirm...\n", seq)
	to := 10 * time.Millisecond
	for {
		decided, v := sm.px.Status(seq)
    DPrintf("Status %t...\n", decided)
		if decided {
			op, _ := v.(Op)
			return op
    }
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

// Make a request on seq.
func (sm *ShardMaster) NewIns(seq int, op Op) Op {
  sm.px.Start(seq, op)
  return sm.Wait(seq)
}

//Load Balancing Function
func (sm *ShardMaster) Rebalance(config *Config, gid int64) {
  nG := len(config.Groups) //Can assume not zero
  if nG == 0 {
    fmt.Printf(" ###########    0 Groups are left. Error...\n")
  }
  avg := NShards/nG
  DPrintf("Current Groups Num is [%d] ...\n", nG)

  //Remove shards for LEAVE request
  for i := 0; i < NShards; i++ {
    if config.Shards[i] == gid {
      config.Shards[i] = 0
    }
  }

  //Get the distribution of the Shards' group
  count := make(map[int64]int)
	for i := 0; i < NShards; i++ {
		count[config.Shards[i]]++
	}
  DPrintf("Current distribution:\n")
  for k,v := range count {
    DPrintf("     Group: %d, Count: %d\n", k, v)
  }

  DPrintf(" Rebalancing...\n")
	for i := 0; i < NShards; i++ {
		if config.Shards[i] == 0 || count[config.Shards[i]] > avg {
			min := -1
			des_group := int64(-1)
      //Find the min count group, always add to it.
			for k := range config.Groups {
        DPrintf("     Group :%d, count: %d\n", k, count[k])
				if des_group == -1 || count[k] < min {
					des_group = k
					min = count[k]
				}
			}
      DPrintf(" Found: Group-%d == MinCount-%d\n", des_group, min)
			if config.Shards[i] != 0 && count[des_group] == avg {
				continue
			}
      DPrintf(" Do Rebalance...\n")
			count[config.Shards[i]]--
			count[des_group]++
			config.Shards[i] = des_group
		}
	}
  
}

//Handle the Op. Include adjusting the config.
func (sm *ShardMaster) Handle(op Op) {
  DPrintf("Handling Op with Type [%s]\n", op.Operation)
  lastConfig := sm.configs[sm.maxConfig]
  var newConfig Config
  newConfig.Groups = make(map[int64][]string)
  newConfig.Num = lastConfig.Num + 1

  //Copy old config's Groups and Shard
  for g, ser := range lastConfig.Groups {
    newConfig.Groups[g] = ser
  }
  for i:= 0; i < NShards; i++ {
    newConfig.Shards[i] = lastConfig.Shards[i]
  }

  //Actual modification on the configs
  switch op.Operation {
    case JOIN:
      args := op.Args.(JoinArgs)
      newConfig.Groups[args.GID] = args.Servers
      sm.Rebalance(&newConfig, 0)
    case LEAVE:
      args := op.Args.(LeaveArgs)
      delete(newConfig.Groups, args.GID)
      sm.Rebalance(&newConfig, args.GID)
    case MOVE:
      args := op.Args.(MoveArgs)
      newConfig.Shards[args.Shard] = args.GID
    case QUERY:
      //Nothing to do.
    default:
      break
  }

  //Only append newConfig when it is updated.
  if op.Operation == JOIN || op.Operation == LEAVE || op.Operation == MOVE {
    sm.configs = append(sm.configs, newConfig)
    sm.maxConfig = sm.maxConfig + 1;
  }

  sm.PrintConfig()
}

//Update the Config using paxos logs, until the max_seq one.
func (sm *ShardMaster) Update(max_seq int, op Op) {

  if sm.seq > max_seq {
    DPrintf("Already up to date...\n")
    return
  }

  for sm.seq <= max_seq {
    decided, v_ := sm.px.Status(sm.seq)
    var v Op
    if !decided {
      v = sm.NewIns(sm.seq, Op{}) // We need to get agreement value is not record true.
    } else {
      v, _ = v_.(Op)
    }

    DPrintf("Updating Seq [%d/%d]...Type: %s\n", sm.seq, max_seq, v.Operation)
    sm.Handle(v)

    sm.seq = sm.seq + 1
  }
  DPrintf("Finish updating all previous history\n")

  sm.px.Done(sm.seq - 1)//I have update to max_Seq stage. Release memory.
}

//Set the new request and update to configs
func (sm *ShardMaster) SetNewLog(next_seq int, op Op) bool {

  v := sm.NewIns(next_seq, op) //Update this new Ins
  if reflect.DeepEqual(v, op) { //Some technique so that can compare Args.
    DPrintf("Compare Success\n")
    DPrintf("New Instance with Seq %d is created...\n", next_seq)
  } else {
    DPrintf("Compare Not Success\n")
    DPrintf("New Log has not been set. Update again...\n")
    return false
  }
  sm.Handle(v)

  sm.seq = sm.seq + 1
  sm.px.Done(sm.seq - 1)
  return true
}

// Process the incoming request.
// Update the Configs using Paxos
func (sm *ShardMaster) ProcessOp(op Op) {

  for {
    maxSeq := sm.px.Max()

    DPrintf("My seq is [%d], cur max is [%d]\n", sm.seq, maxSeq)
    sm.Update(maxSeq, op)
    ok := sm.SetNewLog(maxSeq+1, op)
    if ok { // new request has been done. Otherwise, meaning that the table is not up to date.
      DPrintf("After update: My seq is [%d], cur max is [%d]\n", sm.seq, maxSeq+1)
      break
    }
  }
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  DPrintf("-- Server [%d] is handling Join Request\n", sm.me)
  op := Op{Operation:JOIN, Args: *args}
  sm.ProcessOp(op)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  DPrintf("-- Server [%d] is handling Leave Request\n", sm.me)
  op := Op{Operation:LEAVE, Args: *args}
  sm.ProcessOp(op)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  DPrintf("-- Server [%d] is handling Move Request\n", sm.me)
  op := Op{Operation:MOVE, Args: *args}
  sm.ProcessOp(op)
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  DPrintf("-- Server [%d] is handling Query Request\n", sm.me)

  op := Op{Operation:QUERY, Args: *args}
  sm.ProcessOp(op)

  DPrintf("Searching for Config %d in history\n", args.Num)
  if args.Num == -1 {
    reply.Config = sm.configs[sm.maxConfig]
    return nil
  }
  for i := 0; i < sm.maxConfig; i++ {
    if sm.configs[i].Num == args.Num {
      reply.Config = sm.configs[i]
      return nil
    }
  }
  //In case not found, return lastest as well.
  DPrintf("Not found, return lastest config...\n")
  reply.Config = sm.configs[sm.maxConfig]
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.seq = 0
  sm.maxConfig = 0

  gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})


  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
