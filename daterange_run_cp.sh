#!/bin/bash

## example sh daterange_run_cp.sh 20130101 10
START_DATE=`echo $1`;
NUM_DAYS=$2

for i in `seq 1 $NUM_DAYS`
do
  xdate=$(date -d "${START_DATE}+${i} days" +%Y%m%d)
  echo $xdate
done
