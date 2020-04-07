package raft

import (
	"fmt"
	"time"
)

type a struct {
	time time.Timer
	c    chan int
}

func main() {
	b := &a{}
	b.time = *time.NewTimer(time.Second)
	b.c = make(chan int)
	go func(b *a) {
		for {
			b.c <- 1
			fmt.Println("传值")
			time.Sleep(500 * time.Millisecond)
		}
	}(b)
	for {
		select {
		case <-b.c:
			fmt.Println("111")
		case <-time.After(time.Second):
			fmt.Println("超时")
		}
	}

}
