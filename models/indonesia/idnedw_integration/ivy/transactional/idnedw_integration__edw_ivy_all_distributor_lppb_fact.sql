{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["jj_mnth_id"],
        pre_hook= ["{{build_edw_p_load_lppb_fact_upd()}}","delete from {{this}} where jj_mnth_id = (select cast(to_char(to_date(dateadd ('month',-1,to_date(cast(jj_mnth_id as varchar),'yyyymm'))),'yyyymm') as integer) from {{ source('idnedw_integration', 'edw_time_dim') }} where to_date(cal_date) = to_date(convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)));"]
    )
}}

with itg_all_distributor_sellin_sales_fact as
(
    select * from {{ref('idnitg_integration__itg_all_distributor_sellin_sales_fact')}}
),
edw_time_dim as
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }} 
),
edw_distributor_dim as
(
    select * from {{ref('idnedw_integration__edw_distributor_dim')}}
),
vw_all_distributor_sellout_sales_fact as
(
    select * from {{ref('idnedw_integration__vw_all_distributor_sellout_sales_fact')}}

),
vw_all_distributor_ivy_sellout_sales_fact as
(
    select * from {{ ref('idnedw_integration__vw_all_distributor_ivy_sellout_sales_fact') }}
),
edw_all_distributor_lppb_fact as
(
    select * from {{ source('idnedw_integration', 'edw_all_distributor_lppb_fact') }} 
),
final as
(
    SELECT TRIM(UPPER(JJ_MNTH_ID||JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID)) AS REC_KEY,
       TRIM(UPPER(JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID)) AS CUST_PROD_CD,
       JJ_MNTH_ID,
       TRIM(UPPER(JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
       TRIM(UPPER(JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
       SUM(SELLIN_QTY) AS SELLIN_QTY,
       SUM(SELLOUT_QTY) AS SELLOUT_QTY,
       SUM(STRT_INV_QTY) AS STRT_INV_QTY,
       SUM(STRT_INV_QTY + SELLIN_QTY - SELLOUT_QTY) AS END_INV_QTY,
       SUM(SELLOUT_LAST_TWO_MNTHS_QTY) AS SELLOUT_LAST_TWO_MNTHS_QTY,
       SUM(SELLIN_VAL) AS SELLIN_VAL,
       SUM(SELLOUT_VAL) AS SELLOUT_VAL,
       SUM(STRT_INV_VAL) AS STRT_INV_VAL,
       SUM(STRT_INV_VAL + SELLIN_VAL - SELLOUT_VAL) AS END_INV_VAL,
       SUM(SELLOUT_LAST_TWO_MNTHS_VAL) AS SELLOUT_LAST_TWO_MNTHS_VAL,
       MAX(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) AS CRTD_DTTM,
       MAX(CAST(NULL AS TIMESTAMP)) AS UPTD_DTTM
FROM 
(
--1
SELECT T2.JJ_MNTH_ID,
             TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(QTY) AS SELLIN_QTY,
             SUM(0) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(NET_VAL) AS SELLIN_VAL,
             SUM(0) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM ITG_ALL_DISTRIBUTOR_SELLIN_SALES_FACT T1,
           edw_time_dim T2
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
      AND   T2.JJ_MNTH_ID = (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                             FROM edw_time_dim
                             WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id))   NOT IN (SELECT DISTINCT TRIM(UPPER(jj_sap_dstrbtr_id))                                                                       
                                         FROM edw_distributor_dim
                                         WHERE region='NATIONAL ACCOUNT')
      GROUP BY T2.JJ_MNTH_ID,
               T1.JJ_SAP_DSTRBTR_ID,
               T1.JJ_SAP_PROD_ID
UNION ALL

--2      
SELECT T2.JJ_MNTH_ID,
             TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(0) AS SELLIN_QTY,
             SUM(SLS_QTY) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(0) AS SELLIN_VAL,
             SUM(JJ_NET_VAL) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM 
      (SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_sellout_sales_fact
       where (dstrbtr_grp_cd not in ('DNR','CSA','PON','SAS','RFS','SNC') and (dstrbtr_grp_cd <> 'ADT' or JJ_SAP_DSTRBTR_ID <> '117089'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0
       
UNION ALL
  
       SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from vw_all_distributor_ivy_sellout_sales_fact
       where (dstrbtr_grp_cd in ('DNR','CSA','PON','SAS','RFS','SNC') or (dstrbtr_grp_cd = 'ADT' AND JJ_SAP_DSTRBTR_ID = '117089'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0) T1,
           edw_time_dim T2
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
	  AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL 
      AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID))!='DAOG20'
      AND   T2.JJ_MNTH_ID = (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                             FROM edw_time_dim
                             WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id)) NOT IN (SELECT DISTINCT TRIM(UPPER(jj_sap_dstrbtr_id))
                                         FROM edw_distributor_dim
                                          WHERE region='NATIONAL ACCOUNT')
      GROUP BY T2.JJ_MNTH_ID,
               T1.JJ_SAP_DSTRBTR_ID,
               T1.JJ_SAP_PROD_ID
               
UNION ALL

--3      
SELECT (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
              FROM edw_time_dim
              WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AS JJ_MNTH_ID,
             TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(0) AS SELLIN_QTY,
             SUM(0) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(SLS_QTY) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(0) AS SELLIN_VAL,
             SUM(0) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(JJ_NET_VAL) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM (SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_sellout_sales_fact
       where (dstrbtr_grp_cd not in ('DNR','CSA','PON','SAS','RFS','SNC') and (dstrbtr_grp_cd <> 'ADT' or JJ_SAP_DSTRBTR_ID <> '117089'))
	   --and (dstrbtr_grp_cd <> 'SDN' or JJ_SAP_DSTRBTR_ID <> '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0
       
UNION ALL

SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_ivy_sellout_sales_fact
       where (dstrbtr_grp_cd in ('DNR','CSA','PON','SAS','RFS','SNC') or (dstrbtr_grp_cd = 'ADT' AND JJ_SAP_DSTRBTR_ID = '117089'))
	   --or (dstrbtr_grp_cd = 'SDN' AND JJ_SAP_DSTRBTR_ID = '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0) T1,
           edw_time_dim T2
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
	  AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL 
      AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID))!='DAOG20'
      AND   T2.JJ_MNTH_ID BETWEEN (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-3,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                                   FROM edw_time_dim
                                   WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))) AND (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-2,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                                                                                FROM edw_time_dim
                                                                                WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id)) NOT IN (SELECT DISTINCT TRIM(UPPER(jj_sap_dstrbtr_id))
                                         FROM edw_distributor_dim
                                         WHERE region='NATIONAL ACCOUNT')
      GROUP BY (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                FROM edw_time_dim
                WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))),T1.JJ_SAP_DSTRBTR_ID,
               T1.JJ_SAP_PROD_ID
               
UNION ALL

--4
SELECT (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
              FROM edw_time_dim
              WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AS JJ_MNTH_ID,
             TRIM(UPPER(T1.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(0) AS SELLIN_QTY,
             SUM(0) AS SELLOUT_QTY,
             SUM(END_INV_QTY) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(0) AS SELLIN_VAL,
             SUM(0) AS SELLOUT_VAL,
             SUM(END_INV_VAL) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM EDW_ALL_DISTRIBUTOR_LPPB_FACT T1
      WHERE T1.JJ_MNTH_ID = (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-2,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                             FROM edw_time_dim
                             WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      GROUP BY (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                FROM edw_time_dim
                WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))),T1.JJ_SAP_DSTRBTR_ID,
               T1.JJ_SAP_PROD_ID
               
UNION ALL

--5
SELECT T2.JJ_MNTH_ID,
             T3.AREA AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(QTY) AS SELLIN_QTY,
             SUM(0) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(NET_VAL) AS SELLIN_VAL,
             SUM(0) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM ITG_ALL_DISTRIBUTOR_SELLIN_SALES_FACT T1,
           edw_time_dim T2,
           (SELECT DISTINCT jj_sap_dstrbtr_id,
                   AREA,effective_from,effective_to
            FROM edw_distributor_dim
            WHERE region='NATIONAL ACCOUNT') T3
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
      AND   T2.JJ_MNTH_ID = (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                             FROM edw_time_dim
                             WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id)) = TRIM(UPPER(T3.jj_sap_dstrbtr_id))
	  and   T2.JJ_MNTH_ID between T3.effective_from(+) and T3.effective_to(+) 
      GROUP BY T2.JJ_MNTH_ID,
               T3.AREA,
               T1.JJ_SAP_PROD_ID
               
UNION ALL

--6
SELECT T2.JJ_MNTH_ID,
             T3.AREA AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(0) AS SELLIN_QTY,
             SUM(SLS_QTY) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(0) AS SELLIN_VAL,
             SUM(JJ_NET_VAL) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(0) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM (SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_sellout_sales_fact
       where (dstrbtr_grp_cd not in ('DNR','CSA','PON','SAS','RFS','SNC') and (dstrbtr_grp_cd <> 'ADT' or JJ_SAP_DSTRBTR_ID <> '117089'))
	   --and (dstrbtr_grp_cd <> 'SDN' or JJ_SAP_DSTRBTR_ID <> '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0
       
UNION ALL

SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_ivy_sellout_sales_fact
       where (dstrbtr_grp_cd in ('DNR','CSA','PON','SAS','RFS','SNC') or (dstrbtr_grp_cd = 'ADT' AND JJ_SAP_DSTRBTR_ID = '117089'))
	   --or (dstrbtr_grp_cd = 'SDN' AND JJ_SAP_DSTRBTR_ID = '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0) T1,
           edw_time_dim T2,
           (SELECT DISTINCT jj_sap_dstrbtr_id,
                   AREA,effective_from,effective_to 
            FROM edw_distributor_dim
            WHERE region='NATIONAL ACCOUNT') T3
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
	  AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL 
      AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID))!='DAOG20'
      AND   T2.JJ_MNTH_ID = (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                             FROM edw_time_dim
                             WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id)) = TRIM(UPPER(T3.jj_sap_dstrbtr_id))
	  and   T1.jj_mnth_id between T3.effective_from(+) and T3.effective_to(+) 
      GROUP BY T2.JJ_MNTH_ID,
               T3.AREA,
               T1.JJ_SAP_PROD_ID
               
UNION ALL

--7      
SELECT (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
              FROM edw_time_dim
              WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AS JJ_MNTH_ID,
             T3.AREA AS JJ_SAP_DSTRBTR_ID,
             TRIM(UPPER(T1.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
             SUM(0) AS SELLIN_QTY,
             SUM(0) AS SELLOUT_QTY,
             SUM(0) AS STRT_INV_QTY,
             SUM(0) AS END_INV_QTY,
             SUM(SLS_QTY) AS SELLOUT_LAST_TWO_MNTHS_QTY,
             SUM(0) AS SELLIN_VAL,
             SUM(0) AS SELLOUT_VAL,
             SUM(0) AS STRT_INV_VAL,
             SUM(0) AS END_INV_VAL,
             SUM(JJ_NET_VAL) AS SELLOUT_LAST_TWO_MNTHS_VAL
      FROM (SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_sellout_sales_fact
       where (dstrbtr_grp_cd not in ('DNR','CSA','PON','SAS','RFS','SNC') and (dstrbtr_grp_cd <> 'ADT' or JJ_SAP_DSTRBTR_ID <> '117089'))
	   --and (dstrbtr_grp_cd <> 'SDN' or JJ_SAP_DSTRBTR_ID <> '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0
       
UNION ALL
SELECT trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id,
       SUM(sls_qty) AS SLS_QTY,
       SUM(grs_val) AS GRS_VAL,
       SUM(jj_net_val) AS jj_net_val,
       SUM(trd_dscnt) AS TRD_DSCNT,
       SUM(dstrbtr_net_val) AS DSTRBTR_NET_VAL,
       SUM(rtrn_qty) AS RTRN_QTY,
       SUM(rtrn_val) AS RTRN_VAL from  vw_all_distributor_ivy_sellout_sales_fact
       where (dstrbtr_grp_cd in ('DNR','CSA','PON','SAS','RFS','SNC') or (dstrbtr_grp_cd = 'ADT' AND JJ_SAP_DSTRBTR_ID = '117089'))
	   --or (dstrbtr_grp_cd = 'SDN' AND JJ_SAP_DSTRBTR_ID = '116193'))
       GROUP BY
       trans_key,
       bill_doc,
       bill_dt,
       jj_mnth_id,
       jj_wk,
       dstrbtr_grp_cd,
       dstrbtr_id,
       jj_sap_dstrbtr_id,
       dstrbtr_cust_id,
       dstrbtr_prod_id,
       jj_sap_prod_id,
       dstrbtn_chnl,
       grp_outlet,
       dstrbtr_slsmn_id
       HAVING SUM(jj_net_val) <> 0) T1,
           edw_time_dim T2,
           (SELECT DISTINCT jj_sap_dstrbtr_id,
                   AREA,effective_from,effective_to 
            FROM edw_distributor_dim
            WHERE region='NATIONAL ACCOUNT') T3
      WHERE TO_DATE(T1.BILL_DT) = TO_DATE(T2.CAL_DATE)
	  AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID)) IS NOT NULL 
      AND   TRIM(UPPER(T1.JJ_SAP_PROD_ID))!='DAOG20'
      AND   T2.JJ_MNTH_ID BETWEEN (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-3,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                                   FROM edw_time_dim
                                   WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))) AND (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-2,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                                                                                FROM edw_time_dim
                                                                                WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)))
      AND   TRIM(UPPER(T1.jj_sap_dstrbtr_id)) = TRIM(UPPER(T3.jj_sap_dstrbtr_id))
	  and T1.jj_mnth_id between T3.effective_from(+) and T3.effective_to(+) 
      GROUP BY (SELECT CAST(TO_CHAR(TO_DATE(DATEADD ('MONTH',-1,TO_DATE(CAST(JJ_MNTH_ID AS VARCHAR),'YYYYMM'))),'YYYYMM') AS INTEGER)
                FROM edw_time_dim
                WHERE TO_DATE(CAL_DATE) = TO_DATE(convert_timezone('UTC',current_timestamp())::timestamp_ntz(9))),T3.AREA,
               T1.JJ_SAP_PROD_ID
)
               
GROUP BY JJ_MNTH_ID||JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID,
         JJ_SAP_DSTRBTR_ID||JJ_SAP_PROD_ID,
         JJ_MNTH_ID,
         JJ_SAP_DSTRBTR_ID,
         JJ_SAP_PROD_ID
)

select * from final