#!/bin/bash
set -e
if [ $# -ne 1 ]; then
	echo "Usage: $0 [test] [repeat time]"
	exit 1
fi
# export "GOPATH=$(git rev-parse --show-toplevel)"
# cd "${GOPATH}/src/raft"
#rm -rf temp
#mkdir temp
for ((i=0;i<$1;i++))
do
    echo $i
	time go test -race -run TestBasic
done
#rm -rf temp