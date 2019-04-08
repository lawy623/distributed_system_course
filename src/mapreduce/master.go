package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  mapDone := make(chan bool, mr.nMap)
  reduceDone := make(chan bool, mr.nReduce)

  //Doing Maps (require nMap times.)
  for i := 0; i < mr.nMap; i++ {
    go func (jobID int) {
      for {
        worker := <- mr.registerChannel

        //Use lock to avoid concurrency
        mr.mux.Lock()
        _, exist := mr.Workers[worker]
        mr.mux.Unlock()
        if !exist {
          mr.mux.Lock()
          mr.Workers[worker] = &WorkerInfo{worker}
          mr.mux.Unlock()
        }

        args := &DoJobArgs{}
        var reply DoJobReply
        args.File = mr.file
        args.Operation = Map
        args.JobNumber = jobID
        args.NumOtherPhase = mr.nReduce

        ok := call(worker, "Worker.DoJob", args, &reply)

        if ok == false {
          fmt.Printf("Worker %s doMap error\n", worker)
        } else {
            mapDone <- true
            mr.registerChannel <- worker
            return
        }
      }
    }(i)
  }

  //All Map jobs must finish before doing Reduce
  for i := 0 ; i < mr.nMap; i++ {
    <- mapDone
  }

  fmt.Printf("Finish Map...\n")

  //Doing Reduces (require nReduce times.)
  for i := 0; i < mr.nReduce; i++ {
    go func (jobID int) {
      for {
        worker := <- mr.registerChannel

        //Use lock to avoid concurrency
        mr.mux.Lock()
        _, exist := mr.Workers[worker]
        mr.mux.Unlock()
        if !exist {
          mr.mux.Lock()
          mr.Workers[worker] = &WorkerInfo{worker}
          mr.mux.Unlock()
        }

        args := &DoJobArgs{}
        var reply DoJobReply
        args.File = mr.file
        args.Operation = Reduce;
        args.JobNumber = jobID
        args.NumOtherPhase = mr.nMap

        ok := call(worker, "Worker.DoJob", args, &reply)

        if ok == false {
          fmt.Printf("Worker %s doReduce error\n", worker)
        } else {
            reduceDone <- true
            mr.registerChannel <- worker
            return
        }
      }
    }(i)
  }

  //All Reduce jobs must finish before exit
  for i := 0 ; i < mr.nReduce; i++ {
    <- reduceDone
  }

  fmt.Printf("Finish Reduce...\n")

  /* What I use to check the Workers info is correct */
  //fmt.Printf("Worker size %v...\n", len(mr.Workers))
  //for w := range mr.Workers{
  //  fmt.Printf("Worker's Name %s...\n", mr.Workers[w].address)
  //}

  return mr.KillWorkers()
}
