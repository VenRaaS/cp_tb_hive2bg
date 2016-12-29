#!bash
START_DATE=$1;
NUM_DAYS=$2
echo $START_DATE
echo $NUM_DAYS
for (( i=0; i<$NUM_DAYS; i++ ))
do
  xdate=$(date -d "${START_DATE} +${i} days" +%Y%m%d)
  echo $xdate

  sh ./weblog_hive2bq_3.sh $xdate
done
