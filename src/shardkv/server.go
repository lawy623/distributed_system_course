package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
import "reflect"

type Op struct {
  // Your definitions here.
  Key string
  Value string
  Type string
  DoHash bool
  Name string
  Seq string
  PreVal string
  KV_Store map[string]string
  Last_Reply map[string]string
  ConfigNum int
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  config shardmaster.Config
  kv_store map[string]string
  last_reply map[string]string
  seq int
}

//Helper function to print the configs in servers.
func PrintConfig(v shardmaster.Config) {
  DPrintf(" Printing Configs information...\n")
  DPrintf("   [Num %d] -->", v.Num)
    DPrintf("(%d Shards) [", len(v.Shards))
    for a,_ := range v.Shards {
      DPrintf("%d ", v.Shards[a])
    }
    DPrintf("]->")

  DPrintf("(%d Groups)", len(v.Groups))
  DPrintf("\n")
}

//Wait for the status
func (kv *ShardKV) Wait(seq int) Op {
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

//Update the kv database using paxos logs, until the max_seq one.
func (kv *ShardKV) Update(max_seq int) {
  for kv.seq <= max_seq {
    decided, v_ := kv.px.Status(kv.seq)
    var v Op
    if !decided {
      v = kv.NewIns(kv.seq, Op{}) // We need to get agreement value is not record true.
    } else {
      v, _ = v_.(Op)
    }

    DPrintf("Updating Seq [%d/%d]...Type: %s", kv.seq, max_seq, v.Type)
    if v.Type == "PUT" {  //Only Put needs to update values.
      DPrintf("Updating kv map...\n")
      DPrintf("Log -- [Key: %s, Value: %s, Ops: %t]..\n", v.Key, v.Value, v.DoHash)
      pre_val := kv.kv_store[v.Key]
      if v.DoHash {
        kv.kv_store[v.Key] = strconv.Itoa(int(hash(pre_val + v.Value)))
      } else {
        kv.kv_store[v.Key] = v.Value
      }
      DPrintf("   Prev: [%s]. Now: [%s]...\n", pre_val, kv.kv_store[v.Key])
      kv.last_reply[v.Seq] = pre_val
    } else if v.Type == "RECONFIG" {
      //Add data from other Shard when they are gone. Also Update current Config.
      for key, val := range v.KV_Store {
        kv.kv_store[key] = val
      }
      for seq, val := range v.Last_Reply {
        kv.last_reply[seq] = val
      }
      kv.config = kv.sm.Query(v.ConfigNum)
    }
    kv.seq = kv.seq + 1
  }
  DPrintf("Finish updating all previous history")

  kv.px.Done(kv.seq - 1)//I have update to max_Seq stage. Release memory.
}

//Set the new coming request. If done before, do not update twice.
func (kv *ShardKV) SetNewLog(next_seq int, op Op) bool {
  _, exist := kv.last_reply[op.Seq]
  if !exist {
    v := kv.NewIns(next_seq, op) //Update this new Ins
    if reflect.DeepEqual(v, op) { //v == op can not be used now.
      DPrintf("New Instance with Seq %d is created...\n", next_seq)
    } else {
      DPrintf("New Log has not been set. Update again...\n")
      return false
    }
    if v.Type == "PUT" { //Only Put needs to update values.
      DPrintf("Updating for the new call on kv map...\n")
      DPrintf("Log -- [Key: %s, Value: %s, Ops: %t]..\n", v.Key, v.Value, v.DoHash)
      pre_val := kv.kv_store[v.Key]
      if v.DoHash {
        kv.kv_store[v.Key] = strconv.Itoa(int(hash(pre_val + v.Value)))
      } else {
        kv.kv_store[v.Key] = v.Value
      }
      DPrintf("   Prev: [%s]. Now: [%s]...\n", pre_val, kv.kv_store[v.Key])
      kv.last_reply[v.Seq] = pre_val
    } else if v.Type == "RECONFIG" {
      //Add data from other Shard when they are gone.
      for k, v := range op.KV_Store {
        kv.kv_store[k] = v
      }
      for seq, val := range op.Last_Reply {
        kv.last_reply[seq] = val
      }
      kv.config = kv.sm.Query(op.ConfigNum)
    }
    kv.seq = kv.seq + 1
  }
  kv.px.Done(kv.seq - 1)
  return true
}

// Make a request on seq.
func (kv *ShardKV) NewIns(seq int, op Op) Op {
  kv.px.Start(seq, op)
  return kv.Wait(seq)
}

// Process the incoming request.
// Update the database and make the new operation.
// Also update the current config.
// Avoid make duplicated modification.
func (kv *ShardKV) ProcessOp(op Op) {
  for {
    maxSeq := kv.px.Max()

    DPrintf("My seq is [%d], cur max is [%d]\n", kv.seq, maxSeq)
    kv.Update(maxSeq)
    ok := kv.SetNewLog(maxSeq+1, op)
    if ok { // new request has been done. Otherwise, meaning that the table is not up to date.
      DPrintf("After update: My seq is [%d], cur max is [%d]\n", kv.seq, maxSeq+1)
      break
    }
  }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Get request from User-%s::ID-%s, [Key: %s]... KV server %d is processing\n", args.Name, args.Seq, args.Key, kv.me)
  DPrintf("My Gid is [%d]:...Gid by shard is:[%d]", kv.gid, kv.config.Shards[key2shard(args.Key)])
  op := Op{Key:args.Key, Type:"GET", Name:args.Name, Seq:args.Seq}
  kv.ProcessOp(op)

  if kv.config.Shards[key2shard(args.Key)] != kv.gid {
    DPrintf("Wrong Group! ...\n")
    reply.Err = ErrWrongGroup
    return nil
  }
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

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  DPrintf("Put request from User-%s::ID-%s, [Key: %s, Value: %s, Ops: %t]... KV server %d is processing\n", args.Name, args.Seq, args.Key, args.Value, args.DoHash, kv.me)
  DPrintf("My Gid is [%d]:...Gid by shard is:[%d]", kv.gid, kv.config.Shards[key2shard(args.Key)])

  op := Op{Key:args.Key, Value:args.Value, Type:"PUT", DoHash:args.DoHash, Name:args.Name, Seq:args.Seq}
  kv.ProcessOp(op)

  if kv.config.Shards[key2shard(args.Key)] != kv.gid {
    DPrintf("Wrong Group! ...\n")
    reply.Err = ErrWrongGroup
    return nil
  }

  reply.PreviousValue = kv.last_reply[args.Seq]
  reply.Err = OK

  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // Update to the current status by paxos logs first
  maxSeq := kv.px.Max()
  DPrintf("My seq is [%d], cur max is [%d]\n", kv.seq, maxSeq)
  kv.Update(maxSeq)

  // Ask whether New Config has been update.
  newConfig := kv.sm.Query(kv.config.Num + 1)
  if newConfig.Num == kv.config.Num {
    DPrintf("No change in the Config...\n")
    return
  }

  DPrintf("Config Change...\n")
  //PrintConfig(kv.config)
  //DPrintf("Change to :\n")
  //PrintConfig(newConfig)

  // Some Shards need to be merged to my group.
  var merge []int
  for k,v := range newConfig.Shards {
    //This shard is not with our group before, but now it is with our group. We update it to our data.
    if kv.config.Shards[k] != kv.gid && v == kv.gid && kv.config.Shards[k] > 0 {
        merge = append(merge, k)
    }
  }

  new_kv_store := make(map[string]string)
  new_last_reply := make(map[string]string)

  var done bool
  for _, shard := range merge { // Consider all the shards that we need to copy.
    servers := kv.config.Groups[kv.config.Shards[shard]]
    args := &CopyArgs{newConfig.Num, shard}
    var reply CopyReply
    done = false
    for !done {
      for _, server := range servers { // We copy it from either one in this group handling this shard. That server itself must be up-to-date.
        ok := call(server, "ShardKV.CopyShard", args, &reply)
        if ok && reply.Err == OK {
          DPrintf("Grab Successfully...\n")
          for k,v := range reply.KV_Store {
            new_kv_store[k] = v
          }
          for seq, val := range reply.Last_Reply {
            new_last_reply[seq] = val
          }
          done = true
          break
        }
        time.Sleep(20 * time.Millisecond)
      }
    }
  }

  // If nothing new in last_reply and kv_store, will just update the new config.
  DPrintf("Record down Reconfig operation...Update all...\n")
  op := Op{Type:"RECONFIG", ConfigNum:newConfig.Num, Last_Reply:new_last_reply, KV_Store:new_kv_store, Seq:strconv.Itoa(int(nrand()))}
  kv.ProcessOp(op)
}

// Copy data from other Shard and merge with ours.
func (kv *ShardKV) CopyShard(args *CopyArgs, reply *CopyReply) error {
  //The config view is not up-to-date. Wait for it update to lastest.
  if kv.config.Num < args.LastestConfigNum {
    DPrintf("Server %d not up-to-date...\n", kv.me)
    reply.Err = ErrNotUpdate
    return nil
  }

  new_kv := make(map[string]string)

  // Only those to that shard will be moved.
  for k,v := range kv.kv_store {
    if key2shard(k) == args.Shard {
      new_kv[k] = v
    }
  }

  reply.Err = OK
  reply.KV_Store = new_kv
  reply.Last_Reply = kv.last_reply
  return nil
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
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

  // Your initialization code here.
  // Don't call Join().
  kv.config = kv.sm.Query(0)
  kv.kv_store = make(map[string]string)
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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
