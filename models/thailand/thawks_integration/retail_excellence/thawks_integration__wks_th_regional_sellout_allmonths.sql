with WKS_TH_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('thawks_integration__wks_th_base_retail_excellence') }}
    ),
EDW_VW_CAL_RETAIL_EXCELLENCE_DIM as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
    ),
EDW_VW_OS_TIME_DIM as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
    ),

transformation as (SELECT all_months.cntry_cd,
       all_months.sellout_dim_key,
       all_months.data_src,
       all_months.month,
       CAST(base.SLS_QTY AS NUMERIC(38,6)) AS SLS_QTY,
       base.SLS_VALUE AS SLS_VALUE,
       base.AVG_QTY AS AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             data_src,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key,
                   data_src
            FROM WKS_TH_BASE_RETAIL_EXCELLENCE
            --changed to last_26mnths
            WHERE MNTH_ID >= (SELECT last_36mnths
                              FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC
            AND   mnth_id <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC) a,
           (SELECT DISTINCT "year" AS YEAR,
                   mnth_id AS MONTH
            FROM edw_vw_os_time_dim
            --changed to last_26mnths
            WHERE (MNTH_ID >= (SELECT last_36mnths
                               FROM edw_vw_cal_Retail_excellence_Dim) AND mnth_id <= (SELECT TO_CHAR(DATEADD(DAY, 90, SYSDATE()), 'yyyymm')))) b) all_months
  LEFT JOIN (SELECT *
             FROM WKS_TH_BASE_RETAIL_EXCELLENCE
             --changed to last_26mnths
             WHERE MNTH_ID >= (SELECT last_36mnths
                               FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC
             AND   mnth_id <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC) base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.data_src = base.data_src
        AND all_months.MONTH = base.MNTH_ID),

final as (
    select
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
data_src::varchar(14) AS data_src,
month::varchar(23) AS month,
sls_qty::numeric(38,6) AS sls_qty,
sls_value::numeric(38,6) AS sls_value,
avg_qty::numeric(38,6) AS avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price
from transformation
)        



select * from final