package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.
type MyRPCArgs struct {
	Status string// apply commit
	Type int// 1 map 2 reduce
	Id int

}
type MyRPCReplay struct {
	Type     int
	//Type 1为map
	//Type 2为reduce
	Filename string
	ReduceN int
	MapN int
	Id int
	AllFinished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
