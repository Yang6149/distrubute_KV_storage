#!/bin/bash

rm res -rf
mkdir res
rm -rf temp
mkdir temp
for ((i = 0; i < 100; i++))
do

    for ((c = $((i*6)); c < $(( (i+1)*6)); c++))
    do
         (go test -race ) &> ./res/$c &
         sleep 2
    done

    #sleep 90

    if grep -nr "WARNING.*" res; then
        echo "WARNING: DATA RACE"
    fi
    if grep -nr "FAIL.*raft.*" res; then
        echo "found fail"
    fi
done