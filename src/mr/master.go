package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "time"
import "sync"


type Master struct {
	reduceN int
	mapWork []WorkStatus
	// Your definitions here.
	reduceTable []WorkStatus
	mu sync.RWMutex


}
type WorkStatus struct{
	filename string
	status int// 1为完成 0 为未完成  2 等待
	start int64
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Handler(args *MyRPCArgs, reply *MyRPCReplay) error {

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	for i:=range m.reduceTable{
		if m.reduceTable[i].status!=1{
			return false
		}
	}
	for i:=range m.mapWork{
		if m.mapWork[i].status!=1{
			return false
		}
	}
	// Your code here.

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.reduceN=nReduce
	m.mapWork=make([]WorkStatus,len(files))
	for i:=range files{
		m.mapWork[i]=WorkStatus{files[i],0,0}
	}
	m.reduceTable=make([]WorkStatus,nReduce)
	for i:=0;i<nReduce;i++{
		m.reduceTable[i]=WorkStatus{"mr-out-"+strconv.Itoa(i),0,0}
	}

	// 得到所有原始slide 文件和要确定的reduce数量
	//分配worker：map去查读取这些文件

	m.server()
	return &m
}
