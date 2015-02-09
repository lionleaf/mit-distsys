package mapreduce

import "container/list"
import "fmt"
import "sync"


type WorkerInfo struct {
	address string
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

// Makes sure a specific job is executed and completed by at least one worker.

func DoAsyncJob(mr *MapReduce, args DoJobArgs, wg *sync.WaitGroup){
    returnArgs := DoJobReply{OK: false}
    var workerID string

    for ; !returnArgs.OK ; {
        //Wait for a free worker
        workerID = <-mr.registerChannel

        call(workerID,"Worker.DoJob", args, &returnArgs)
        if !returnArgs.OK{
            fmt.Printf("Async job failed!");
        }
    }
    //Notify the workgroup and let the worker do more work!
    wg.Done()
    mr.registerChannel <- workerID
}

func (mr *MapReduce) RunMaster() *list.List {
    //Waitgroup so we can syncronize after map and reduce
    var wg sync.WaitGroup

    //Start all Map jobs
    for i:=0 ; i<mr.nMap;i++ {
        args := DoJobArgs{File:mr.file,
                        Operation:Map,
                        JobNumber:i,
                        NumOtherPhase:mr.nReduce}
        wg.Add(1);
        go DoAsyncJob(mr, args, &wg)
    }

    //Wait for all map jobs to complete
    wg.Wait();

    //Start all Reduce jobs
    for i:=0 ; i<mr.nReduce;i++ {
        args := DoJobArgs{File:mr.file,
                        Operation:Reduce,
                        JobNumber:i,
                        NumOtherPhase:mr.nMap}
        wg.Add(1);
        go DoAsyncJob(mr, args, &wg)
    }

    //Wait for all Reduce jobs
    wg.Wait();

	return mr.KillWorkers()
}
