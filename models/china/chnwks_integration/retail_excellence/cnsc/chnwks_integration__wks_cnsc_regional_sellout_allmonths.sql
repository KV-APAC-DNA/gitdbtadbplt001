{{ 
    config(
    sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )}}
with WKS_CNSC_BASE_RETAIL_EXCELLENCE as (
    select * from {{ ref('chnwks_integration__wks_cnsc_base_retail_excellence') }}
),
edw_vw_cal_Retail_excellence_Dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

transformation as (SELECT all_months.cntry_cd,
       all_months.sellout_dim_key,
       all_months.month,
       CAST(base.SLS_QTY AS NUMERIC(38,6)) AS SLS_QTY,
       base.SLS_VALUE,
       base.AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key
            FROM WKS_CNSC_BASE_RETAIL_EXCELLENCE) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM edw_vw_os_time_dim
            WHERE MNTH_ID >= 
            --CHANGED MONTH LOGIC 37 -> 28
            (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)
            ---mnth_id > TO_CHAR(add_months(SYSDATE,-36),'YYYYMM')
            AND mnth_id <= (SELECT TO_CHAR(DATEADD(DAY, 90, SYSDATE()), 'yyyymm'))
            ) b) all_months
  LEFT JOIN WKS_CNSC_BASE_RETAIL_EXCELLENCE base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.MONTH = base.MNTH_ID),

final as (
select 
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
sls_qty::numeric(38,6) AS sls_qty,
sls_value::numeric(38,6) AS sls_value,
avg_qty::numeric(38,6) AS avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price
from transformation
)

select * from final        