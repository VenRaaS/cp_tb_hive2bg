#!/bin/bash
export EX_DATE='20160229'     #$(date --date="${1-yesterday}" +%Y%m%d)
export EX_DATE10=$(date --date="${1-yesterday}" +%Y-%m-%d)
export TABLE_NM="weblog_"${EX_DATE}
export HIVE_TMP_DB="itri"
export BQ_TMP_DB='itri'
export BQ_DB='nono_Unima'
export HDFS_TMP_DIR='/user/itri'/${TABLE_NM}
export GS_TMP_DIR='gs://venraastest-bq-upload'/${TABLE_NM}
export GCP_PROJECT_ID='venraas-test'

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

#begin
echo "create Hive table -${HIVE_TMP_DB}.${TABLE_NM}"
hive -e "create table ${HIVE_TMP_DB}.${TABLE_NM}
    STORED AS AVRO
    LOCATION '${HDFS_TMP_DIR}'
    AS
    select uid,page_type,action,client_host,tophost,categ_le,categ_code,gid,trans_id,ilist,now_rec,from_rec,cc_session,cc_guid,ven_session,ven_guid,client_ip,country,browser,unix_timestamp(api_logtime) as api_logtime,client_utc,client_tzo,community,rating,coords_lon,coords_lat,cc_web,api_loghost,optional,agent,device_viewtype,device_id,uri,para,referrer,unix_timestamp(sys_update_time) as sys_update_time
    from momo_unima_20160623.all_weblog
    where to_date(api_logtime) = from_unixtime(unix_timestamp('${EX_DATE}' ,'yyyyMMdd'), 'yyyy-MM-dd')"
# get file from hdfs
echo "=>get hdfs files ${HDFS_TMP_DIR} to ./${TABLE_NM}"
hadoop fs -get ${HDFS_TMP_DIR} .
# upload to gs
echo "=>upload file to gs ${GS_TMP_DIR}"
gsutil cp ./${TABLE_NM}/* ${GS_TMP_DIR}
echo "=>create bq temp table - ${BQ_TMP_DB}.${TABLE_NM}- with external gs source - ${GS_TMP_DIR}/*"
bq load --project_id ${GCP_PROJECT_ID} --source_format=AVRO  ${BQ_TMP_DB}.${TABLE_NM} ${GS_TMP_DIR}/*
echo "=>create bq table ${BQ_DB}.${TABLE_NM}"
#echo "bq query --project_id ${GCP_PROJECT_ID} --nouse_legacy_sql --allow_large_results --destination_table=${BQ_DB}.${TABLE_NM} "select uid,page_type,action,client_host,tophost,categ_le,categ_code,gid,trans_id,ilist,now_rec,from_rec,cc_session,cc_guid,ven_session,ven_guid,client_ip,country,browser,DATETIME(TIMESTAMP_SECONDS(api_logtime), '+08:00') as api_logtime,client_utc,client_tzo,community,rating,coords_lon,coords_lat,cc_web,api_loghost,optional,agent,device_viewtype,device_id,uri,para,referrer,DATETIME(TIMESTAMP_SECONDS(sys_update_time), '+08:00') as sys_update_time FROM" ${BQ_TMP_DB}.${TABLE_NM}"
bq query --project_id ${GCP_PROJECT_ID} --nouse_legacy_sql --allow_large_results --destination_table=${BQ_DB}.${TABLE_NM} "select uid,page_type,action,client_host,tophost,categ_le,categ_code,gid,trans_id,ilist,now_rec,from_rec,cc_session,cc_guid,ven_session,ven_guid,client_ip,country,browser,DATETIME(TIMESTAMP_SECONDS(api_logtime), '+08:00') as api_logtime,client_utc,client_tzo,community,rating,coords_lon,coords_lat,cc_web,api_loghost,optional,agent,device_viewtype,device_id,uri,para,referrer,DATETIME(TIMESTAMP_SECONDS(sys_update_time), '+08:00') as sys_update_time FROM ${BQ_TMP_DB}.${TABLE_NM}"
                                                                                                                                  
# clean up
echo "=>delete local temp folder ./${TABLE_NM}"
rm -r ./${TABLE_NM}/
echo "=>drop Hive temp table ${HIVE_TMP_DB}.${TABLE_NM}"
hive -e "drop table ${HIVE_TMP_DB}.${TABLE_NM}"
echo "=>remove gs temp folder ${GS_TMP_DIR}"
gsutil rm -r  ${GS_TMP_DIR}
echo "=>drop bq temp table ${BQ_TMP_DB}.${TABLE_NM}"
bq rm --project_id ${GCP_PROJECT_ID} -f -t ${BQ_TMP_DB}.${TABLE_NM}
#echo "=>drop bq table ${BQ_DB}.${TABLE_NM}"
#bq rm --project_id ${GCP_PROJECT_ID} -f -t ${BQ_DB}.${TABLE_NM}
