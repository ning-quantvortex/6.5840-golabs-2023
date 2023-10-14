package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MRWorker struct {
	// expires_at    time.Time
	id            int
	mf            func(string, string) []KeyValue
	rf            func(string, []string) string
	task          Task
	intermediates []string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		w := MRWorker{
			mf: mapf,
			rf: reducef,
			id: os.Getegid(),
		}

		task, nReduce, ok := w.callRequestTask()
		if !ok {
			log.Println("the RPC call failed")
			return
		}

		if task.id == 0 {
			log.Println("failed to get the task")
			return
		}
		w.task = task

		if task.status == Completed {
			return
		}

		if task.taskType == MapTask {
			files, err := w.dealMapWork(task, nReduce)
			w.intermediates = files
			if w.callMapTaskDone(files, err) {
				return
			}
		} else if task.taskType == ReduceTask {
			w.dealReduceWork(task)
			if w.callReduceTaskDone() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func (w *MRWorker) callRequestTask() (Task, int, bool) {

	// declare an argument structure.
	args := Args{
		mrWorker: *w,
	}

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		fmt.Printf("reply is: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return Task{}, 0, false
	}
	return reply.Task, reply.NReduce, true
}

func (w *MRWorker) callMapTaskDone(files []string, err error) bool {
	args := Args{}

	reply := MapTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if ok {
		fmt.Printf("reply is: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
	if reply.ShouldExit {
		return true
	}
	return false
}

func (w *MRWorker) callReduceTaskDone() bool {
	args := Args{}

	reply := ReduceTaskDoneReply{}
	ok := call("Coordinator.MapTaskDone", &args, &reply)
	if ok {
		fmt.Printf("reply is: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
	if reply.ShouldExit {
		return true
	}
	return false
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (w *MRWorker) dealMapWork(task Task, nReduce int) ([]string, error) {
	filename := task.path
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot open %v", filename))
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := w.mf(filename, string(content))

	// create tmp intermediate files
	files := make([]*os.File, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		path := fmt.Sprintf("tmp/mr-%v-%v-%v", task.assignee, i, os.Getegid())
		file, err := os.Create(path)
		if err != nil {
			log.Fatalf("Failed to create file for path: %v", path)
		}
		files = append(files, file)

		// use io buffer to reduce the disk read / write
		// buffer := bufio.NewWriter(file)
		// encoders = append(encoders, json.NewEncoder(buf))
	}

	sort.Sort(ByKey(kva))

	for _, kv := range kva {
		idx := ihash(kv.Key)
		writeFile := files[idx]
		encoder := json.NewEncoder(writeFile)
		err := encoder.Encode(&kv)
		if err != nil {
			return nil, err
		}

	}

	// flush file buffer to disk
	// for i, buf := range buffers {
	// 	err := buf.Flush()
	// }

	// rename the tmp file names
	filenames := []string{}
	for i, f := range files {
		f.Close()
		newPath := fmt.Sprintf("tmp/mr-%v-%v-%v", task.assignee, i)
		err := os.Rename(f.Name(), newPath)
		if err != nil {
			log.Fatalf("Failed to rename the tmp file, the old path is: %v", f.Name())
		}
		filenames = append(filenames, f.Name())
	}
	return filenames, nil
}

func (w *MRWorker) dealReduceWork(task Task) {

}

// func writeToFile(filename string, content []byte) error {
// 	// If the file doesn't exist, create it, or append to the file
// 	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	if _, err := f.Write(content); err != nil {
// 		f.Close() // ignore error; Write error takes precedence
// 		log.Fatal(err)
// 	}
// 	if err := f.Close(); err != nil {
// 		log.Fatal(err)
// 	}
// }
