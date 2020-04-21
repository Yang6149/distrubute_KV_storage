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
	#go test -race -run TestBasic3A
    #go test -race -run TestConcurrent3A
    #go test -race -run TestUnreliable3A
    #go test -race -run TestUnreliableOneKey3A
    #go test -race -run TestOnePartition3A
    #go test -race -run TestManyPartitionsOneClient3A
    #go test -race -run TestManyPartitionsManyClients3A
    #go test -race -run TestPersistOneClient3A
    #go test -race -run TestPersistConcurrentUnreliable3A
    #go test -race -run TestPersistPartition3A
    #go test -race -run TestPersistPartitionUnreliable3A
    #go test  -run TestPersistPartitionUnreliableLinearizable3A
    time go test  -run 3A
	#cat logfile.log > temp/$i
	#rm logfile.log
done
#rm -rf temp