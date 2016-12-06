#!/bin/bash

# clean up
echo "=>delete local temp folder ./${TABLE_NM}"
rm -r ./${TABLE_NM}/
echo "=>drop Hive temp table ${HIVE_TMP_DB}.${TABLE_NM}"
hive -e "drop table ${HIVE_TMP_DB}.${TABLE_NM}"
echo "=>remove gs temp folder ${GS_TMP_DIR}"
gsutil rm -r  ${GS_TMP_DIR}
echo "=>drop bq temp table ${BQ_TMP_DB}.${TABLE_NM}"
bq rm --project_id ${GCP_PROJECT_ID}t -f -t ${BQ_TMP_DB}.${TABLE_NM}
echo "=>drop bq table ${BQ_DB}.${TABLE_NM}"
bq rm --project_id ${GCP_PROJECT_ID} -f -t ${BQ_DB}.${TABLE_NM}
