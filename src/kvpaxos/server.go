package kvpaxos

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
import "strconv"

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Key string
  Value string
  Type string
  DoHash bool
  Name string
  Seq string
  PreVal string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kv_store map[string]string
  done map[string]bool
  last_reply map[string]string
  seq int
}

//Wait for the status
func (kv *KVPaxos) Wait(seq int) Op {
  DPrintf("Waiting for Seq %d to confirm...\n", seq)
	to := 10 * time.Millisecond
	for {
		decided, v := kv.px.Status(seq)
    DPrintf("Status %t...\n", decided)
		if decided {
			kv, _ := v.(Op)
			return kv
    }
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

// Help function to print paxos logs
func (kv *KVPaxos) PrintPaxosHis() {
  his := kv.px.GetHis()
  len := len(his)

  DPrintf("   Current len of paxos history instance of kv server %d is %d...\n", kv.me, len)
  for k, v := range his {
    DPrintf("   [Instance %d | Status %t]\n", k, v)
  }
  DPrintf("\n")
}

//Update the kv database using paxos logs, until the max_seq one.
func (kv *KVPaxos) Update(max_seq int, op Op) {
  for kv.seq <= max_seq {
    decided, v_ := kv.px.Status(kv.seq)
    var v Op
    if !decided {
      v = kv.NewIns(kv.seq, Op{}) // We need to get agreement value is not record true.
    } else {
      v, _ = v_.(Op)
    }

    DPrintf("Updating Seq [%d/%d]...Type: %s", kv.seq, max_seq, v.Type)
    if v.Type != "GET" {  //Only Put needs to update values.
      DPrintf("Updating kv map...\n")
      DPrintf("Log -- [Key: %s, Value: %s, Ops: %t]..\n", v.Key, v.Value, v.DoHash)
      pre_val := kv.kv_store[v.Key]
      if v.DoHash {
        kv.kv_store[v.Key] = strconv.Itoa(int(hash(pre_val + v.Value)))
      } else {
        kv.kv_store[v.Key] = v.Value
      }
      DPrintf("   Prev: [%s]. Now: [%s]...\n", pre_val, kv.kv_store[v.Key])
      kv.last_reply[v.Name] = pre_val
    }
    kv.done[v.Seq] = true
    kv.seq = kv.seq + 1
  }
  DPrintf("Finish updating all previous history")

  kv.px.Done(kv.seq - 1)//I have update to max_Seq stage. Release memory.
}

//Set the new coming request. If done before, do not update twice.
func (kv *KVPaxos) SetNewLog(next_seq int, op Op) bool {
  _, exist := kv.done[op.Seq]
  if !exist {
    v := kv.NewIns(next_seq, op) //Update this new Ins
    if v == op {
      DPrintf("New Instance with Seq %d is created...\n", next_seq)
    } else {
      DPrintf("New Log has not been set. Update again...\n")
      return false
    }
    if v.Type != "GET" { //Only Put needs to update values.
      DPrintf("Updating for the new call on kv map...\n")
      DPrintf("Log -- [Key: %s, Value: %s, Ops: %t]..\n", v.Key, v.Value, v.DoHash)
      pre_val := kv.kv_store[v.Key]
      if v.DoHash {
        kv.kv_store[v.Key] = strconv.Itoa(int(hash(pre_val + v.Value)))
      } else {
        kv.kv_store[v.Key] = v.Value
      }
      DPrintf("   Prev: [%s]. Now: [%s]...\n", pre_val, kv.kv_store[v.Key])
      kv.last_reply[v.Name] = pre_val
    }
    kv.done[v.Seq] = true
    kv.seq = kv.seq + 1
  }
  kv.px.Done(kv.seq - 1)
  return true
}

// Make a request on seq.
func (kv *KVPaxos) NewIns(seq int, op Op) Op {
  kv.px.Start(seq, op)
  return kv.Wait(seq)
}

// Process the incoming request.
// Update the database and make the new operation.
// Avoid make double modification.
func (kv *KVPaxos) ProcessOp(op Op) {
  for {
    maxSeq := kv.px.Max()

    DPrintf("My seq is [%d], cur max is [%d]\n", kv.seq, maxSeq)
    kv.Update(maxSeq, op)
    ok := kv.SetNewLog(maxSeq+1, op)
    if ok { // new request has been done. Otherwise, meaning that the table is not up to date.
      DPrintf("After update: My seq is [%d], cur max is [%d]\n", kv.seq, maxSeq+1)
      break
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  DPrintf("Get request from %s, [Key: %s]... KV server %d is processing\n", args.Seq, args.Key, kv.me)

  op := Op{Key:args.Key, Type:"GET", Name:args.Name, Seq:args.Seq}
  kv.ProcessOp(op)

  //If we update the dataset to the lastest version, it should be fine.
  v, ok := kv.kv_store[args.Key]
  if !ok {
    DPrintf("Get Not Found!!!!!!!\n")
    reply.Err = ErrNoKey
  } else {
    DPrintf("Get Found Value [%s].....\n", v)
    reply.Err = OK
    reply.Value = v
  }

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  DPrintf("Put request from %s, [Key: %s, Value: %s, Ops: %t]... KV server %d is processing\n", args.Seq, args.Key, args.Value, args.DoHash, kv.me)


  op := Op{Key:args.Key, Value:args.Value, Type:"PUT", DoHash:args.DoHash, Name:args.Name, Seq:args.Seq}
  kv.ProcessOp(op)

  reply.PreviousValue = kv.last_reply[args.Name]
  reply.Err = OK
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kv_store = make(map[string]string)
  kv.done = make(map[string]bool)
  kv.last_reply = make(map[string]string)
  kv.seq = 0

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}
