#!/bin/bash
export EX_DATE=$(date --date="${1-yesterday}" +%Y%m%d)
export EX_DATE10=$(date --date="${EX_DATE}" +%Y-%m-%d)
export EX_MONTH=$(date --date="${EX_DATE}" +%Y-%m)
export TABLE_NM="orderlist_"${EX_DATE}
export HIVE_TMP_DB="itri"
export BQ_TMP_DB='itri'
export BQ_DB='nono_Unima'
export HDFS_TMP_DIR='/user/itri'/${TABLE_NM}
export GS_TMP_DIR='gs://venraastest-bq-upload'/${TABLE_NM}
export GCP_PROJECT_ID='venraas-test'
export SQL_SELECT_FROM_HIVE="create table ${HIVE_TMP_DB}.${TABLE_NM}
    STORED AS AVRO
    LOCATION '${HDFS_TMP_DIR}'
    AS
    select uid,order_no,seq,unix_timestamp(order_date) as order_date,gid,currency,sale_price,final_price,qty,final_amt,promo_id,affiliate_id,dc_price,delivery_type,unix_timestamp(update_time) as update_time
    from angel.all_orderlist
    where log_mon_i='${EX_MONTH}' AND  to_date(order_date) = '${EX_DATE10}'"
export SQL_SELECT_INTO_BQ="select uid,order_no,seq,DATETIME(TIMESTAMP_SECONDS(order_date), '+08:00') as order_date,gid,currency,sale_price,final_price,qty,final_amt,promo_id,affiliate_id,dc_price,delivery_type,DATETIME(TIMESTAMP_SECONDS(update_time), '+08:00') as update_time FROM ${BQ_TMP_DB}.${TABLE_NM}"

sh ./cleanup_tmp.sh
sh ./cp_tb_hive2bg.sh
sh ./cleanup_tmp.sh
