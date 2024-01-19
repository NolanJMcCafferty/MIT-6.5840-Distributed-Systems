package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	SolicitWork(mapf, reducef)
}

//
// function to poll the coordinator for a work task
//
func SolicitWork(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := SolicitWorkArgs{}
		reply := SolicitWorkReply{}

		response := call("Coordinator.AssignWork", &args, &reply)

		if !response {
			break
		}

		if reply.WorkType == "map" {
			executeMap(
				reply.Filename,
				reply.TaskNumber,
				reply.TotalTasks,
				mapf)
		} else if reply.WorkType == "reduce" {
			executeReduce(
				reply.TaskNumber,
				reply.TotalTasks,
				reducef)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

//
// execute the Map operation on the given file,
// and store the results in nReduce intermediate files
//
func executeMap(
	filename string,
	taskNumber int,
	nReduce int,
	mapf func(string, string) []KeyValue) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	intermediate := mapf(filename, string(content))

	for i := 0; i < nReduce; i++ {
		intermediateFilename := makeIntermediateFilename(taskNumber, i)

		intermediateFile, _ := ioutil.TempFile(".", intermediateFilename)

		enc := json.NewEncoder(intermediateFile)
		for _, kv := range intermediate {
			if (ihash(kv.Key) % nReduce) == i {
				err := enc.Encode(&kv)

				if err != nil {
					log.Fatalf("cannot write %v", intermediateFile.Name())
				}
			}
		}
		intermediateFile.Close()

		_ = os.Remove(intermediateFilename)
		os.Rename(intermediateFile.Name(), intermediateFilename)
	}
}

//
// execute the Reduce operation on the relevant intermediate files,
// and write the results to a output file
//
func executeReduce(
	taskNumber int,
	nMap int,
	reducef func(string, []string) string) {

	intermediate := gatherIntermediateData(taskNumber, nMap)

	sort.Sort(ByKey(intermediate))

	outfileName := fmt.Sprintf("mr-out-%v", taskNumber)
	outfile, _ := ioutil.TempFile(".", outfileName)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	outfile.Close()

	_ = os.Remove(outfileName)
	os.Rename(outfile.Name(), outfileName)
}

func gatherIntermediateData(
	taskNumber int,
	nMap int) []KeyValue {

	intermediate := []KeyValue{}

	for i := 0; i < nMap; i++ {
		filename := makeIntermediateFilename(i, taskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}
	}

	return intermediate
}

func makeIntermediateFilename(mapTaskNumber int, reduceTaskNumber int) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskNumber, reduceTaskNumber)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	return false
}

