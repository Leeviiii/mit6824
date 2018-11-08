package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	// the code is not correct why??? go test -run Basic
	/*var wait_group sync.WaitGroup;
	for taskNumber := 0; taskNumber < ntasks;taskNumber++ {
		var mapFile string
		if phase == mapPhase {
			mapFile = mapFiles[taskNumber]
		} else {
			mapFile = ""
		}
		wait_group.Add(1)
		go func(mapFile string) {
			defer wait_group.Done()
			for {
				workerAddress := <-registerChan
				fmt.Printf("get the registerChan %s\n", workerAddress)
				args := DoTaskArgs{JobName: jobName, File: mapFile, Phase: phase, TaskNumber: taskNumber, NumOtherPhase: n_other}
				ok := call(workerAddress, "Worker.DoTask", args, nil)
				if ok == false {
					fmt.Printf("DoWorker: RPC %s dotask error\n", workerAddress)
				} else {
					go func(){registerChan<-workerAddress}()
					//registerChan<-workerAddress
					break
				}
			}
		}(mapFile)
	}
	wait_group.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)*/
    /*
     * wait_group 控制着整个这次schedule 必须都完成才可以
     *
     * go func(){registerChan<-workerAddress}() 这里放回很关键，这里不放回下次就没办法在渠道workerAddress
     * 就没办法并行。 我第一个版本就没有放回，导致串行，test偶尔会爆出有一些节点没有任何task的错误
     */
	var wait_group sync.WaitGroup;
	for taskNumber := 0; taskNumber < ntasks;taskNumber++ {
		args := DoTaskArgs{JobName: jobName, Phase: phase, TaskNumber: taskNumber, NumOtherPhase: n_other}
		if phase == mapPhase {
			args.File = mapFiles[taskNumber]
		}
		wait_group.Add(1)
		go func() {
			defer wait_group.Done()
			for {
				workerAddress := <-registerChan
				fmt.Printf("get the registerChan %s\n", workerAddress)
				ok := call(workerAddress, "Worker.DoTask", args, nil)
				if ok == false {
					fmt.Printf("DoWorker: RPC %s dotask error\n", workerAddress)
				} else {
					go func(){registerChan<-workerAddress}()
					//registerChan<-workerAddress
					break
				}
			}
		}()
	}
	wait_group.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)

}
