package main

import "../mr"
import "plugin"
import "os"
import "log"

func main(){
	for i:=0;i<10;i++{
		response:=mr.MyRPCReplay{}
		response.Type=2
		response.MapN=8
		response.Filename="mr-out-"+string(i+48)
		response.Id=i
		_, reducef := loadPlugin(os.Args[1])
		mr.ReduceImp(response,reducef)
	}

}
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
