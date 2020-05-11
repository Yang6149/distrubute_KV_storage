

rm res -rf
mkdir res
set int j = 0
for ((i = 0; j < 25; i++))
do
    for ((c = $((i*10)); c < $(( (i+1)*10)); c++))
    do
         (go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B) &> ./res/$c &
    done

    sleep 40

    if grep -nr "FAIL.*raft.*" res; then
        echo "fail"
    fi

done