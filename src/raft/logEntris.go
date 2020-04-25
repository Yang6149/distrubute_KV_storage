package raft

import "fmt"

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) logGet(index int) Entry {
	fmt.Println("get", rf.me, index, rf.lastIncludedIndex, rf.logLen())
	return rf.log[index-rf.lastIncludedIndex-1]
}

func (rf *Raft) logGets(begin int, end int) []Entry {
	return rf.log[begin-rf.lastIncludedIndex-1 : end-rf.lastIncludedIndex-1]
}

func (rf *Raft) logSet(index int, e Entry) {
	rf.log[index-rf.lastIncludedIndex-1] = e
}

func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex + 1
}

func (rf *Raft) logTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	if index-rf.lastIncludedIndex-1 < 0 || index-rf.lastIncludedIndex-1 >= len(rf.log) {
		fmt.Println(rf.me, "*-*-*-", index, rf.lastIncludedIndex, len(rf.log))
	}
	return rf.log[index-rf.lastIncludedIndex-1].Term
}

func (rf *Raft) logDiscard(index int) {
	fmt.Println("++", rf.me, len(rf.log), rf.logLen()-index-1, index, rf.logLen())
	rf.log = rf.logGets(index+1, rf.logLen())
}
