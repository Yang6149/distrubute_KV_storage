package raft

import "sync"
import "time"
import "log"

type a struct{
	value int
	mu sync.Mutex
}

func main() {
	rf := &a{}
	for i:=0;i<1000;i++{
		go rf.inc()
	}
	time.Sleep(1*time.Second)
	log.Println(rf.value)
}

func (rf *a)inc(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.value++
}

func (rf *a)dec(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.value--
}
