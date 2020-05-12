#!/bin/bash
set -e
if [ $# -ne 1 ]; then
	echo "Usage: $0 [test] [repeat time]"
	exit 1
fi
# export "GOPATH=$(git rev-parse --show-toplevel)"
# cd "${GOPATH}/src/raft"
rm -rf temp
mkdir temp
for ((i=0;i<$1;i++))
do
    echo $i
	#time go test -run TestJoinLeave
	#time go test -run TestStaticShards
	#time go test -run TestSnapshot
	#time go test -run TestMissChange
	#time go test -run TestConcurrent1
	#time go test -run TestConcurrent2
	#time go test -run TestUnreliable1
	#time go test -run TestUnreliable2
	#time go test -run TestUnreliable3
	#time go test -run TestChallenge1Delete
	#time go test -run TestChallenge1Concurrent
	#time go test -run TestChallenge2Unaffected
	#time go test -run TestChallenge2Partial
	time go test 

	cat logfile.log > temp/a$i
	rm logfile.log
done
#rm -rf temp