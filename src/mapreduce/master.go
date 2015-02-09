package mapreduce

import "container/list"
import "fmt"
import "sync"


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
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func DoAsyncJob(mr *MapReduce, args DoJobArgs, wg *sync.WaitGroup){
    defer fmt.Printf("Job complete. wg : %s. job nr: %s\n", wg, args.JobNumber)
    var returnArgs DoJobReply
    fmt.Printf("Waiting for worker for job nr %s\n", args.JobNumber)
    workerID := <-mr.registerChannel
    fmt.Printf("Staring Job %s on Worker ID: %s\n", args.JobNumber, workerID)
    call(workerID,"Worker.DoJob", args, &returnArgs)
    if !returnArgs.OK{
        fmt.Printf("Async job failed!");
    }
    wg.Done()
    mr.registerChannel <- workerID
}

func (mr *MapReduce) RunMaster() *list.List {
    var wg sync.WaitGroup

    for i:=0 ; i<mr.nMap;i++ {
        args := DoJobArgs{File:mr.file,
                        Operation:Map,
                        JobNumber:i,
                        NumOtherPhase:mr.nReduce}
        wg.Add(1);
        go DoAsyncJob(mr, args, &wg)
	    fmt.Printf("Started map %d\n", i)
    }

	fmt.Printf("All map started. Waiting\n")
    wg.Wait();

	fmt.Printf("Map complete\n")
    for i:=0 ; i<mr.nReduce;i++ {
        args := DoJobArgs{File:mr.file,
                        Operation:Reduce,
                        JobNumber:i,
                        NumOtherPhase:mr.nMap}
        wg.Add(1);
        go DoAsyncJob(mr, args, &wg)
	    fmt.Printf("Started reduce %d\n", i)
    }
    wg.Wait();

	fmt.Printf("Reduce complete\n")

	return mr.KillWorkers()
}
