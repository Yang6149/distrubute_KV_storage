package raft

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) logGet(index int) Entry {
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
	DPrintf("%d index=%d ,lastIncludedIndex = %d ,lastIncludedTerm", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm)
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	//fmt.Println(rf.me, "++++", index-rf.lastIncludedIndex-1, len(rf.log), index, rf.lastIncludedIndex)
	return rf.log[index-rf.lastIncludedIndex-1].Term
}

func (rf *Raft) logDiscard(index int) {
	//现在还是上一个的 lastIncluded，因为logGets，要使用
	DPrintf("%d ,discard[%d:%d]", rf.me, index+1, rf.logLen())
	rf.log = rf.logGets(index+1, rf.logLen())
	// if len(rf.nextIndex) != 0 {
	// 	for i := range rf.peers {
	// 		if i == rf.me {
	// 			continue
	// 		}
	// 		if i >= len(rf.nextIndex) {
	// 			fmt.Println(i, rf.nextIndex)
	// 			fmt.Println("!!!!!!!")
	// 		}
	// 		fmt.Println(rf.lastIncludedIndex)
	// 		//rf.nextIndex[i]-1=preIndex
	// 		//pre-lastIncludex-1 = realIndex
	// 		if rf.nextIndex[i]-1-index-1 >= len(rf.log) {
	// 			fmt.Println(rf.me, "**", rf.nextIndex[i], index, len(rf.log))
	// 			fmt.Println(rf.log[rf.nextIndex[i]-1-index-1])
	// 		}
	// 	}
	// }

}
