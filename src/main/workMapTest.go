package main

import "../mr"
import "plugin"
import "os"
import "log"

func main(){
	mapf, _ := loadPlugin(os.Args[1])

	for i, filename := range os.Args[2:] {
		response:=mr.MyRPCReplay{}
		response.Type=1
		response.Filename=filename
		response.ReduceN=10
		response.Id=i
		mr.MapImp(response,mapf)
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