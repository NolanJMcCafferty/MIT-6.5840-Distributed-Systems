# MIT-6.5840-Distribute-Systems

This is the set of labs from the MIT 6.5840 Distributed Systems class which is available for free on [MIT OpenCourseWare](https://pdos.csail.mit.edu/6.824/index.html). 
I followed this online course independently during the 2023-2024 winter.  

## Lab 1: MapReduce

[Lab Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

In this lab I modified `mr/coordinator.go`, `mr/worker.go` and `mr/rpc.go` to complete the MapReduce implementation.

![mr_lab_tests.png](mr_lab_tests.png)

Use the following commands to run the test suite:

```$xslt
$ git clone https://github.com/NolanJMcCafferty/MIT-6.5840-Distribute-Systems.git
$ cd MIT-6.5840-Distribute-Systems/src/main
$ bash test-mr.sh
```

and to run the test suite `x` times in a row:

```$xslt
$ bash test-mr-many.sh x
```

## Lab 2: Raft

[Lab Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

In this lab I modified `raft/raft.go` and `raft/rpc.go`.

![raft lab tests png](raft_lab_tests.png)

Use the following commands to run the test suite:

```$xslt
$ git clone https://github.com/NolanJMcCafferty/MIT-6.5840-Distribute-Systems.git
$ cd MIT-6.5840-Distribute-Systems/src/raft
$ go test
```

## Lab 3: Fault-tolerant Key/Value Service

[Lab Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

In this lab I modified `kvraft/client.go`, `kvraft/server.go`, and `kvraft/common.go` to complete the Key/Value service implementation.

![kvraft_lab_tests.png](kvraft_lab_tests.png)

Use the following commands to run the test suite:

```$xslt
$ git clone https://github.com/NolanJMcCafferty/MIT-6.5840-Distribute-Systems.git
$ cd MIT-6.5840-Distribute-Systems/src/kvraft
$ go test
```