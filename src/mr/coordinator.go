package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	mu sync.Mutex          // Lock to protect shared access
	files []string
	nReduce int
	mapTasksCompleted []bool
	reduceTasksCompleted []bool
}

//
// RPC handler to assign a task to a worker
//
func (c *Coordinator) AssignWork(args *SolicitWorkArgs, reply *SolicitWorkReply) error {
	c.mu.Lock()

	if c.mustWaitForMapTasksToComplete() {
		c.mu.Unlock()
		return nil
	}

	if !c.intermediateFilesExist() {
		reply.WorkType = "map"
		mapTask := c.chooseMapTask()
		reply.TaskNumber = mapTask
		reply.Filename = c.files[mapTask]
		reply.TotalTasks = c.nReduce

		c.mapTasksCompleted[mapTask] = true
		c.mu.Unlock()

		go c.ensureMapTaskIsCompleted(mapTask, c.nReduce)

		return nil
	}

	if c.mustWaitForReduceTasksToComplete() {
		c.mu.Unlock()
		return nil
	}

	if !c.outputFilesExist() {
		reply.WorkType = "reduce"
		reduceTask := c.chooseReduceTask()
		reply.TaskNumber = reduceTask
		reply.TotalTasks = len(c.mapTasksCompleted)

		c.reduceTasksCompleted[reduceTask] = true
		c.mu.Unlock()

		go c.ensureReduceTaskIsCompleted(reduceTask)

		return nil
	}

	return errors.New("MapReduce is done")
}

func (c * Coordinator) mustWaitForMapTasksToComplete() bool {
	return !c.intermediateFilesExist() && c.allMapTasksStarted()
}

func (c * Coordinator) chooseMapTask() int {
	return c.chooseAvailableTask(c.mapTasksCompleted)
}

func (c * Coordinator) allMapTasksStarted() bool {
	return c.chooseMapTask() == -1
}

func (c * Coordinator) intermediateFilesExist() bool {
	for i := 0; i < len(c.files); i++ {
		for j := 0; j < c.nReduce; j++ {
			filename := fmt.Sprintf("mr-%v-%v", i, j)
			if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
	}

	return true
}

func (c *Coordinator) ensureMapTaskIsCompleted(taskNumber int, nReduce int) {
	filename := fmt.Sprintf("mr-%v-%v", taskNumber, nReduce)
	c.ensureTaskIsCompleted(
		taskNumber,
		c.mapTasksCompleted,
		filename)
}

func (c * Coordinator) mustWaitForReduceTasksToComplete() bool {
	return !c.outputFilesExist() && c.allReduceTasksStarted()
}

func (c *Coordinator) chooseReduceTask() int {
	return c.chooseAvailableTask(c.reduceTasksCompleted)
}

func (c *Coordinator) allReduceTasksStarted() bool {
	return c.chooseReduceTask() == -1
}

func (c * Coordinator) outputFilesExist() bool {
	for i := 0; i < c.nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%v", i)
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			return false
		}
	}

	return true
}

func (c *Coordinator) ensureReduceTaskIsCompleted(taskNumber int) {
	filename := fmt.Sprintf("mr-out-%v", taskNumber)
	c.ensureTaskIsCompleted(
		taskNumber,
		c.reduceTasksCompleted,
		filename)
}

//
// Wait and confirm that the task was completed,
// in case the worker crashed or was unable to complete the task
//
func (c *Coordinator) ensureTaskIsCompleted(
	taskNumber int,
	tasks []bool,
	filename string) {

	time.Sleep(15 * time.Second)

	c.mu.Lock()
	if !fileExists(filename) {
		tasks[taskNumber] = false
	}
	c.mu.Unlock()
}

func (c *Coordinator) chooseAvailableTask(tasks []bool) int {
	for i, isCompleted := range tasks {
		if !isCompleted {
			return i
		}
	}

	return -1
}

func fileExists(filename string) bool {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		return false
	}

	return true
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.outputFilesExist()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.files = files
	c.nReduce = nReduce

	for i := 0; i < len(c.files); i++ {
		c.mapTasksCompleted = append(c.mapTasksCompleted, false)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasksCompleted = append(c.reduceTasksCompleted, false)
	}

	c.server()
	return &c
}

