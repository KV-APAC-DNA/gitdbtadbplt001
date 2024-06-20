with edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_singapore_base_retail_excellence as (
    select * from {{ ref('sgpwks_integration__wks_singapore_base_retail_excellence') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
--final cte
singapore_regional_sellout_allmonths  as (
SELECT ALL_MONTHS.CNTRY_CD,		--// SELECT all_months.cntry_cd,
       ALL_MONTHS.SELLOUT_DIM_KEY,		--//        all_months.sellout_dim_key,
       ALL_MONTHS.MONTH,		--//        all_months.month,
       BASE.SO_SLS_QTY,		--//        base.SO_SLS_QTY,
       BASE.SO_SLS_VALUE,		--//        base.SO_SLS_VALUE,
       BASE.SO_AVG_QTY,		--//        base.SO_AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE		--//        BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key
            FROM WKS_SINGAPORE_BASE_RETAIL_EXCELLENCE where MNTH_ID >= (SELECT last_26mnths
                        FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC
      AND   MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM edw_vw_os_time_dim
            WHERE MNTH_ID >= (SELECT last_26mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		--//                  FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)
AND   MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months // SYSDATE
  LEFT JOIN (select * from WKS_SINGAPORE_BASE_RETAIL_EXCELLENCE where MNTH_ID >= (SELECT last_26mnths
                        FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC
      AND   MNTH_ID <= (SELECT prev_mnth FROM edw_vw_cal_Retail_excellence_Dim)::NUMERIC) base
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD		--//          ON all_months.cntry_cd = base.cntry_cd
        AND ALL_MONTHS.SELLOUT_DIM_KEY = BASE.SELLOUT_DIM_KEY		--//         AND all_months.sellout_dim_key = base.sellout_dim_key
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID		--//         AND all_months.MONTH = base.MNTH_ID

),
final as 
(
    select 
     cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_qty::numeric(38,6) AS so_sls_qty,
so_sls_value::numeric(38,6) AS so_sls_value,
so_avg_qty::numeric(38,6) AS so_avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price
from singapore_regional_sellout_allmonths
)

--final select
select * from final

