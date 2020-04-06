package raft

type Entry struct {
	Command interface{}
	Term    int
}
