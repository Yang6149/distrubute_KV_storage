package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "time"
import "sync"
import "context"


const(
	isworking = 1
	idle = 2
	commited = 3


)
type Master struct {
	timeout time.Duration
	reduceN int
	mapN int
	mapWork []WorkStatus//
	reduceTable []WorkStatus
	mu sync.RWMutex
	allFinished bool



}
type WorkStatus struct{
	filename string
	status int// 1为完成 0 为未完成  2 等待
	start int64
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Handler(args *MyRPCArgs, reply *MyRPCReplay) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.Status=="apply"{
		reply.MapN=m.mapN
		reply.ReduceN=m.reduceN
		reply.allFinished=m.allFinished
		//当分配完任务后直接返回，让worker退出
		if m.allFinished{
			return nil
		}
		//开始分配map任务
		for i :=range m.mapWork{
			if m.mapWork[i].status==commited{
				continue
			}
			//当任务还没开始或任务超时时，分配任务
			if m.mapWork[i].status==idle||m.mapWork[i].status==0{
				m.mapWork[i].status=isworking
				reply.Type=1
				reply.Id=i
				reply.Filename=m.mapWork[i].filename
				//设立超时，当前任务超过十秒没有完成任务
				ctx, _ := context.WithTimeout(context.Background(), m.timeout)
				go func() {
					select {
					case <-ctx.Done():
						{
							m.mu.Lock()
							//再次判断如果任务没有完成则状态改为 dile
							if m.mapWork[i].status!=commited{
								m.mapWork[i].status=idle
							}
							m.mu.Unlock()
						}
					}
				}()
			}
			return nil
		}
		//接下来是reduce部分
		for i :=range m.reduceTable{
			if m.reduceTable[i].status==commited{
				continue
			}
			//当任务还没开始或任务超时时，分配任务
			if m.reduceTable[i].status==idle||m.reduceTable[i].status==0{
				m.reduceTable[i].status=isworking
				reply.Type=2
				reply.Id=i
				reply.Filename="mr-out-"+strconv.Itoa(i)
				//设立超时，当前任务超过十秒没有完成任务
				ctx, _ := context.WithTimeout(context.Background(), m.timeout)
				go func() {
					select {
					case <-ctx.Done():
						{
							m.mu.Lock()
							//再次判断如果任务没有完成则状态改为 dile
							if m.reduceTable[i].status!=commited{
								m.reduceTable[i].status=idle
							}
							m.mu.Unlock()
						}
					}
				}()
			}
			return nil
		}
	}
	if args.Status=="commit"{
		if args.Type==1{
			m.mapWork[args.Id].status=commited
		}else{
			m.reduceTable[args.Id].status=commited
		}
		bo := true
		for i:=range m.mapWork{
			if m.mapWork[i].status!=commited {
				bo=false
				break
			}
		}
		for i:=range m.reduceTable{
			if m.reduceTable[i].status!=commited {
				bo=false
				break
			}
		}
		m.allFinished=bo
	}
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


	// Your code here.

	return m.allFinished
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	mapWork:=make([]WorkStatus,len(files))
	for i:=range files{
		mapWork[i]=WorkStatus{files[i],0,0}
	}
	reduceTable:=make([]WorkStatus,nReduce)
	for i:=0;i<nReduce;i++{
		reduceTable[i]=WorkStatus{"mr-out-"+strconv.Itoa(i),0,0}
	}
	m := Master{
		timeout : 	10*time.Second,
		reduceN:	nReduce,
		mapN:		len(files),
		mapWork:	mapWork,
		reduceTable:reduceTable,
		allFinished:false,
	}


	// 得到所有原始slide 文件和要确定的reduce数量
	//分配worker：map去查读取这些文件

	m.server()
	return &m
}
