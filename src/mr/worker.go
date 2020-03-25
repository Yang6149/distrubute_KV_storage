package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"
import "os"
import "strconv"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
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
	//接收一个map函数和一个reduce函数
	//调用一个call的包装方法里面定义好arg和reply
	for{
		response:=CallExample()
		if response.allFinished{
			return
		}
		if response.Type==1{
			MapImp(response,mapf)
			CallFinish(response.Id,response.Type)
		}else if response.Type==2{
			ReduceImp(response,reducef)
			CallFinish(response.Id,response.Type)
		}else if response.Type==0{
			return
		}
		time.Sleep(time.Second)
	}

}

func MapImp(response MyRPCReplay,mapf func(string, string) []KeyValue){
	//分配map
	//读取文件
	file, err := os.Open(response.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", response.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", response.Filename)
	}
	file.Close()
	kva := mapf(response.Filename, string(content))
	//分配kva 写到 rm-X-Y file 中
	reduceN:=response.ReduceN
	//初始化数组
	splitKey := make([][]KeyValue,reduceN)
	for i:=range splitKey{
		splitKey[i]=[]KeyValue{}
	}
	//把kva分配进每个splitKey[i]
	for _,kv:=range kva{
		y:=ihash(kv.Key)%response.ReduceN
		splitKey[y]=append(splitKey[y],kv)
	}
	for i:=range splitKey{
		Wfile, _ :=os.Create("mr-"+strconv.Itoa(response.Id)+"-"+strconv.Itoa(i))
		enc := json.NewEncoder(Wfile)
		for _,kv := range splitKey[i]{
			err:=enc.Encode(kv)
			if err!=nil{
				break
			}
		}
		Wfile.Close()
	}
}

func ReduceImp(response MyRPCReplay,reducef func(string, []string) string){
	//创建filename

	id:=response.Id
	mapNum:=response.MapN
	kva := []KeyValue{}
	for i:=0;i<mapNum;i++{
		filename:="mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(id)
		file,err:=os.Open(filename)
		if err!=nil{
			fmt.Println(err)
			break
		}
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err := dec.Decode(&kv);err!=nil{
				break
			}
			kva = append(kva,kv)
		}

	}

	sort.Sort(ByKey(kva))
	i := 0
	_, err := os.Lstat(response.Filename)
	if !os.IsNotExist(err){
		return
	}
	ofile, _ := os.Create(response.Filename)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		//fmt.Println(values)
		//fmt.Println("啊啊啊")
		output := reducef(kva[i].Key, values)
		//fmt.Println(output)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	//等待全部完成进行删除
	for i:=0;i<mapNum;i++{
		filename:="mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(id)
		os.Remove(filename)
	}
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() MyRPCReplay{

	// declare an argument structure.
	args := MyRPCArgs{}

	// fill in the argument(s).
	args.Status = "apply"

	// declare a reply structure.
	reply := MyRPCReplay{}

	// send the RPC request, wait for the reply.
	if !call("Master.Handler", &args, &reply){
		//拨通 并且返回了 reply
		reply.Type=0
		return reply
	}

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v %v\n", reply.Type,reply.Filename)
	return reply
}
func CallFinish(id int,Type int)MyRPCReplay{
	// declare an argument structure.
	args := MyRPCArgs{}

	// fill in the argument(s).
	args.Status = "commit"
	args.Id=id
	args.Type=Type

	// declare a reply structure.
	reply := MyRPCReplay{}

	// send the RPC request, wait for the reply.
	if !call("Master.Handler", &args, &reply){
		//拨通 并且返回了 reply
		reply.Type=0
		return reply
	}

	// reply.Y should be 100.
	//fmt.Printf("reply.Y %v %v\n", reply.Type,reply.Filename)
	return reply
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()
	//rpc调用master中的方法rpcname为要调用的方法名
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
