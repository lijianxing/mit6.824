package mapreduce

import (
	"fmt"
	"sync"
)

const (
	TASK_IDLE  = 0
	TASK_DOING = 1
	TASK_DONE  = 2
)

type TaskData struct {
	sync.Mutex

	jobName      string
	phase        jobPhase
	file         string
	taskIdx      int
	otherTaskNum int
	worker       string
	status       int // 0: idle 1:doing 2:done
}

func NewTaskData(jobName string, phase jobPhase, taskIdx int, otherTaskNum int, file string) *TaskData {
	return &TaskData{
		jobName:      jobName,
		phase:        phase,
		taskIdx:      taskIdx,
		otherTaskNum: otherTaskNum,
		file:         file,
		status:       TASK_IDLE,
	}
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
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

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	taskData := make([]*TaskData, ntasks)
	for i := 0; i < ntasks; i++ {
		var file string
		if phase == mapPhase {
			file = mapFiles[i]
		} else {
			file = reduceName(jobName, i, n_other)
		}
		taskData[i] = NewTaskData(jobName, phase, i, n_other, file)
	}

	var workerList []string

	workerDoneChan := make(chan string)

	doneTask := 0
	for doneTask < ntasks {
		select {
		case worker := <-registerChan:
			fmt.Printf("worker register:%s\n", worker)

			workerList = append(workerList, worker)
			assignTask(worker, taskData, workerDoneChan)
		case worker := <-workerDoneChan:
			fmt.Printf("worker job done:%s\n", worker)

			doneTask = doneTask + 1
			if doneTask < ntasks {
				assignTask(worker, taskData, workerDoneChan)
			}
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}

func assignTask(worker string, taskData []*TaskData, doneChan chan string) {
	for _, task := range taskData {
		task.Lock()
		defer task.Unlock()

		if task.status == TASK_IDLE {
			task.worker = worker
			task.status = TASK_DOING

			fmt.Printf("assign task %d to worker %s\n", task.taskIdx, task.worker)

			go func(task *TaskData) {
				var taskArgs DoTaskArgs
				taskArgs.JobName = task.jobName
				taskArgs.Phase = task.phase
				taskArgs.TaskNumber = task.taskIdx
				taskArgs.NumOtherPhase = task.otherTaskNum
				taskArgs.File = task.file

				if call(worker, "Worker.DoTask", taskArgs, new(struct{})) {
					fmt.Printf("task %d to worker %s done\n", task.taskIdx, task.worker)

					task.Lock()
					defer task.Unlock()
					task.status = TASK_DONE
					doneChan <- task.worker
				} else {
					// reset task
					task.worker = ""
					task.status = TASK_IDLE
					fmt.Printf("task %d to worker %s failed\n", task.taskIdx, task.worker)
				}
			}(task)
			break
		}
	}
}
