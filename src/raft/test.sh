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
	#go test -run TestBasicAgree2B
	#go test -run TestFigure8Unreliable2C
	#go test -race -run TestRejoin2B
	#time go test -run TestBackup2B
	#go test -race -run TestFailAgree2B
	#time go test -run 2B
	time go test 
	cat logfile.log > temp/$i
	rm logfile.log
done
#rm -rf temp