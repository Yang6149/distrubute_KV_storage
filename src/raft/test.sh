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
	#go test -race -run TestFailNoAgree2B
	#go test -race -run TestFailAgree2B
	#go test -race -run TestFailNoAgree2B
	#go test -race -run TestConcurrentStarts2B
	#go test -race -run TestRejoin2B
	#go test -race -run TestBasicAgree2B
	#go test -race -run TestFailAgree2B
	go test -race -run 2B
	cat logfile.log > temp/$i
	rm logfile.log
done