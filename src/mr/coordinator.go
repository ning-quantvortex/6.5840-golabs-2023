package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	Idle Status = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
)

type Coordinator struct {
	mTasks Tasks
	rTasks Tasks
	// mc is the map channel handles the in progress map tasks
	mc chan Task
	// rc is the reduce channel handles the in progress reduce tasks
	rc chan Task
	// wc               chan Worker
	// rNum is the number of reduce tasks
	rNum int
	// interMap maintains the locations of all intermediate files
	interMap InterMap
}

type InterMap struct {
	interMap map[int][]string
	mu       sync.RWMutex
}

type Tasks struct {
	tasks map[int]Task
	mu    sync.RWMutex
}

type TaskType int
type Status int
type Task struct {
	id            int
	taskType      TaskType
	status        Status
	assignee      int
	path          string
	intermediates []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RequestTask(args *Args, reply *Reply) error {
	return nil
}

func (c *Coordinator) CallCompleteTask()

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("The coordinator begins serve")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		rNum: nReduce,
		mc:   make(chan Task, len(files)),
		rc:   make(chan Task, nReduce),
		mTasks: Tasks{
			tasks: make(map[int]Task),
		},
		rTasks: Tasks{
			tasks: make(map[int]Task),
		},
	}
	c.createMapTasks(files)
	c.server()
	return &c
}

func (c *Coordinator) createMapTasks(files []string) {
	for i, f := range files {
		t := Task{
			id:       i,
			taskType: MapTask,
			status:   Idle,
			path:     f,
		}
		c.mc <- t
		c.mTasks.tasks[t.id] = t
	}
}
