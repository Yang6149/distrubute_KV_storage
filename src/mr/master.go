package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Handler(args *ExampleArgs, reply *ExampleArgs) error {
	//处理参数，处理回复
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 得到所有原始slide 文件和要确定的reduce数量
	//分配worker：map去查读取这些文件

	m.server()
	return &m
}
