package raft

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) logGet(index int) Entry {
	DPrintf("%d : index is %d, real is %d", rf.me, index, index-rf.lastIncludedIndex-1)
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
	DPrintf("%d  rf.lastIn: %d index: %d", index-rf.lastIncludedIndex-1, rf.lastIncludedIndex, index)
	return rf.log[index-rf.lastIncludedIndex-1].Term
}

func (rf *Raft) logDiscard(index int) {
	rf.log = rf.logGets(index+1, rf.logLen())
}
