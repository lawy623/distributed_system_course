package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  mu sync.Mutex
  view viewservice.View
  kv map[string]string
  preReq map[string]map[string]string
  //preHash string
  updateBackup bool
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func IsHandle(pb *PBServer, args *PutArgs) bool {
  _, exist := pb.preReq[args.Name]
  if !exist {
    pb.preReq[args.Name] = make(map[string]string)
  }
  _, exist2 := pb.preReq[args.Name][args.Value]
  //fmt.Printf("Old value is %s... New Value is %s]\n", value, args.Value)
  //return Contains(value,args.Value)
  return exist2
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // Also update the backup to keep a replica database.
  if pb.view.Primary == pb.me && pb.view.Backup != "" {
    //fmt.Printf("Copying primary to %s...\n",pb.view.Backup)
    ok := call(pb.view.Backup, "PBServer.Put", args, reply)
    if !ok || reply.Err != OK { //|| reply.PreviousValue != pb.kv[args.Key] {
        pb.updateBackup = true
        //fmt.Printf("Always wrong here...\n")
        //fmt.Printf("Reply: %s...ok: %t...Prev is:%s\n", reply.Err, ok, reply.PreviousValue)
        reply.Err = ErrWrongServer
        reply.PreviousValue = ""
        return nil
    }
  }

  //fmt.Printf("Clerk %s is sending the request [Key: %s, Value: %s] ...\n", args.Name, args.Key, args.Value)
  if pb.view.Primary != pb.me && pb.view.Backup != pb.me {
    //fmt.Printf("Primary: %s...Backup: %s\n...Me is:%s", pb.view.Primary , pb.view.Backup, pb.me)
    reply.Err = ErrWrongServer
    return nil
  }

  if IsHandle(pb, args) {
    //fmt.Printf("Clerk %s, [Key: %s, Value: %s] is handled before...\n", args.Name, args.Key, args.Value)
    reply.Err = OK
    reply.PreviousValue = pb.preReq[args.Name][args.Value]//pb.preHash
    return nil
  }

  temp := args.Value
  if args.DoHash {
    preV, ok := pb.kv[args.Key]
    if !ok {
      preV = ""
    }
    args.Value = strconv.Itoa(int(hash(preV + args.Value)))
    //fmt.Printf("%s now doing [PreV: %s, Value: %s] =>> %s\n", pb.me, preV, temp, args.Value)
  }

  reply.PreviousValue = pb.kv[args.Key]
  //pb.preHash = pb.kv[args.Key]
  pb.preReq[args.Name][temp] = pb.kv[args.Key]//append(pb.preReq[args.Name][args.Value], temp)
  pb.kv[args.Key] = args.Value

  /*if pb.view.Primary == pb.me {
    fmt.Printf("Primary %s KV to [%s]\n", pb.me, pb.kv)
  } else if pb.view.Backup == pb.me  {
    fmt.Printf("Backup %s KV to [%s]\n", pb.me, pb.kv)
  }*/

  reply.Err = OK
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  // Also Get from the backup to check
  if pb.view.Primary == pb.me && pb.view.Backup != "" {
    //fmt.Printf("Copying primary to %s...\n",pb.view.Backup)
    ok := call(pb.view.Backup, "PBServer.Get", args, reply)
    if !ok || reply.Err != OK || reply.Value != pb.kv[args.Key] {
        pb.updateBackup = true
        reply.Err = ErrWrongServer
        reply.Value = ""
        return nil
    }
  }

  if pb.view.Primary != pb.me && pb.view.Backup != pb.me {
    reply.Err = ErrWrongServer
    return nil
  }
  //if pb.view.Primary == pb.me {
  //  fmt.Printf("Getting Primary %s KV to [%s]\n", pb.me, pb.kv)
  //}
  value, ok := pb.kv[args.Key]
  if ok {
    reply.Value = value
  } else {
    reply.Value = ""
  }

  reply.Err = OK
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  view, err := pb.vs.Ping(pb.view.Viewnum)
  if err != nil {
    fmt.Errorf("Ping(%v) failed", pb.view.Viewnum)
  }

  // A new backup comes up, we want to copy current database to it.
  // If the backup is always there, will be update using Put().
  if view.Primary == pb.me && view.Backup != "" && view.Backup != pb.view.Backup {
    pb.updateBackup = true
  }

  pb.view = view

  if pb.updateBackup {
    //fmt.Printf("Update Backup...\n")
    args := &UpdateBackupArgs{KV: pb.kv, PreReq: pb.preReq}//, PreHash: pb.preHash}
    var reply UpdateBackupReply
    ok := call(view.Backup, "PBServer.UpdateBackup", args, &reply)
    if !ok {
      //fmt.Printf("Update Backup Fail...\n")
      //ok = call(view.Backup, "PBServer.UpdateBackup", args, &reply)
    }
    pb.updateBackup = false
    //fmt.Printf("Update Finish...\n")
  }

}

func (pb *PBServer) UpdateBackup(args *UpdateBackupArgs, reply * UpdateBackupReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  view, err := pb.vs.Ping(pb.view.Viewnum)
  if err != nil {
    fmt.Errorf("Ping(%v) failed", pb.view.Viewnum)
  }

  if view.Backup != pb.me {
    //fmt.Printf("I am not the backup now...\n")
    reply.Err = ErrWrongServer
    return nil
  }

  pb.kv = args.KV
  pb.preReq = args.PreReq
  //pb.preHash = args.PreHash
  //fmt.Printf("Update Backup %s to %s...\n", pb.me, pb.kv)
  reply.Err = OK
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
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.View{Primary: "", Backup: "", Viewnum: 0}
  pb.kv = make(map[string]string)
  pb.preReq = make(map[string]map[string]string)
  //pb.preHash = ""
  pb.updateBackup = false

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
