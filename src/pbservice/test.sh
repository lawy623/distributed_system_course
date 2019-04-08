rm -f test.txt
for i in {1..50}
do
  starttime=$(date +%s)
  echo "Test Run $i"
  go test >> test.txt
  count=`cat ./test.txt | grep Passed | wc -l`
  let "total=13*$i"
  echo "correct count: $count / $total"
  endtime=$(date +%s)
  cost=$((endtime - starttime))
  echo "Time for run $i is $cost's...\n"
done
