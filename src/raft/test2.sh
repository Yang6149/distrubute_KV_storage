#!/bin/bash

export GOPATH="/home/yangshen/MIT6824/6.824"
export PATH="$PATH:/usr/lib/go-1.10/bin"

rm -rf res
mkdir res
rm -rf temp
mkdir temp
for ((i = 0; i < 100; i++))
do

    for ((c = $((i*6)); c < $(( (i+1)*6)); c++))
    do
         (go test -race -run TestFailNoAgree2B) &> ./res/$c &
         sleep 15
    done

    sleep 90

    if grep -nr "WARNING.*" res; then
        echo "WARNING: DATA RACE"
    fi
    if grep -nr "FAIL.*raft.*" res; then
        echo "found fail"
    fi
    cat logfile.log > temp/$i
	rm logfile.log
done