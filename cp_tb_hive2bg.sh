#!/bin/bash

echo "=>create Hive table -${HIVE_TMP_DB}.${TABLE_NM}"
hive -e "${SQL_SELECT_FROM_HIVE}"
# get file from hdfs
echo "=>get hdfs files ${HDFS_TMP_DIR} to ./${TABLE_NM}"
hadoop fs -get ${HDFS_TMP_DIR} .
# upload to gs
echo "=>upload file to gs ${GS_TMP_DIR}"
gsutil cp ./${TABLE_NM}/* ${GS_TMP_DIR}
echo "=>create bq temp table - ${BQ_TMP_DB}.${TABLE_NM}- with external gs source - ${GS_TMP_DIR}/*"
bq load --project_id ${GCP_PROJECT_ID} --source_format=AVRO  ${BQ_TMP_DB}.${TABLE_NM} ${GS_TMP_DIR}/*
echo "=>create bq table ${BQ_DB}.${TABLE_NM}"
bq query --project_id ${GCP_PROJECT_ID} --nouse_legacy_sql --allow_large_results --destination_table=${BQ_DB}.${TABLE_NM} "${SQL_SELECT_INTO_BQ}"
