package main

import (
	"fmt"
	"time"
)

type a struct {
	time time.Timer
}

func main() {
	b := &a{}
	b.time = *time.NewTimer(time.Second)
	go func(b *a) {
		select {
		case <-b.time.C:
			fmt.Println("b")
		}
	}(b)
	b.time.Stop()
	b.time.Reset(3 * time.Second)
	time.Sleep(2 * time.Second)
	fmt.Println("a")
	time.Sleep(2 * time.Second)
}
