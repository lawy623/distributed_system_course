
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  view View   //current view
  idleServer string   //Idle client ready for backup
  lastPingTime map[string]time.Time  //Last ping times of servers
  hasAck bool
}

func (vs *ViewServer) ViewChange(primary string, backup string) {
  //fmt.Printf("ChangeView...%s becomes primary, %s becomes backup...\n",primary,backup)
  vs.view.Primary = primary
  vs.view.Backup = backup
  vs.view.Viewnum += 1
  vs.hasAck = false
}
//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  client, pinNum := args.Me, args.Viewnum
  //fmt.Printf("%s is pinging %v...\n",client,pinNum)

  vs.lastPingTime[client] = time.Now()//update the ping time

  switch client {
    case vs.view.Primary:
      if pinNum == 0 { //Primary reboot
        vs.ViewChange(vs.view.Backup, "")
      } else if pinNum == vs.view.Viewnum {
        vs.hasAck = true
      }
    case vs.view.Backup:
      if pinNum == 0 && vs.hasAck { //Backup reboot
        vs.ViewChange(vs.view.Primary, vs.idleServer)
      }
    default:
      if vs.view.Viewnum == 0 { //Nothing is in view
        vs.ViewChange(client, "")
      } else {
        vs.idleServer = client
      }
  }
  reply.View = vs.view
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.view
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  //Update idleServer to backup server if missing.
  //Idle can not take up primary as it has been initialized as backup.
  if time.Now().Sub(vs.lastPingTime[vs.idleServer]) >= DeadPings*PingInterval {
    vs.idleServer = ""
  } else if vs.view.Backup == "" && vs.hasAck == true {
    vs.ViewChange(vs.view.Primary, vs.idleServer)
    vs.idleServer = ""
  }

  //If Primary is down, let backup takes it. Also update backup as if there is.
  //If Primary has not ACK, will not affect.
  if time.Now().Sub(vs.lastPingTime[vs.view.Primary]) >= DeadPings*PingInterval {
    if vs.hasAck == true {
      vs.ViewChange(vs.view.Backup, vs.idleServer)
      vs.idleServer = ""
    }
  }

  //If Backup is down, let Idle takes it.If Primary has not ACK, will not affect.
  if time.Now().Sub(vs.lastPingTime[vs.view.Backup]) >= DeadPings*PingInterval {
    if vs.hasAck == true && vs.idleServer != "" {
      vs.ViewChange(vs.view.Primary, vs.idleServer)
      vs.idleServer = ""
    }
  }
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

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.view = View{Primary:"", Backup:"", Viewnum:0}
  vs.hasAck = false;
  vs.lastPingTime = make(map[string]time.Time)
  vs.idleServer = ""

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
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
