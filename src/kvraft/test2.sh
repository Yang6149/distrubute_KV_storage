

rm  -rf res
mkdir res
set int j = 0
for ((i = 0; i < 25; i++))
do
    for ((c = $((i*10)); c < $(( (i+1)*10)); c++))
    do
        #  (go test -run TestSnapshotSize3B) &> ./res/$c &
        (go test ) &> ./res/$c &
    done

    sleep 300

    if grep -nr "FAIL.*raft.*" res; then
        echo "fail"
    fi

done