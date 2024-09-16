with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_korea_base_retail_excellence as (
    select * from {{ ref('ntawks_integration__wks_korea_base_retail_excellence') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
--final cte
wks_korea_regional_sellout_allmonths  as (
SELECT ALL_MONTHS.CNTRY_CD,		--// SELECT all_months.cntry_cd,
       ALL_MONTHS.DIM_KEY,		--//        all_months.dim_key,
       ALL_MONTHS.DATA_SOURCE,		--//        all_months.data_source,
       ALL_MONTHS.MONTH,		--//        all_months.month,
       BASE.SO_SLS_QTY,		--//        base.SO_SLS_QTY,
       BASE.SO_SLS_VALUE,		--//        base.SO_SLS_VALUE,
       BASE.SO_AVG_QTY,		--//        base.SO_AVG_QTY,
	   BASE.SALES_VALUE_LIST_PRICE		--// 	   base.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             dim_key,
             data_source,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   dim_key,
                   data_source
            FROM WKS_KOREA_BASE_RETAIL_EXCELLENCE		--//             FROM NA_WKS.WKS_KOREA_BASE_RETAIL_EXCELLENCE
			WHERE MNTH_ID >= (SELECT last_27mnths
                              FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC		--//                               FROM rg_edw.edw_vw_cal_Retail_excellence_Dim)::NUMERIC
            AND   MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC) a,		            
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM EDW_VW_OS_TIME_DIM		--//             FROM os_edw.edw_vw_os_time_dim
			WHERE MNTH_ID >= (SELECT last_27mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
AND   MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months 
  LEFT JOIN (SELECT * FROM WKS_KOREA_BASE_RETAIL_EXCELLENCE		
				WHERE   MNTH_ID >= (SELECT last_27mnths
                              FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC		
            AND   MNTH_ID <= (SELECT prev_mnth FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::NUMERIC) base		
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD		
        AND ALL_MONTHS.DIM_KEY = BASE.DIM_KEY		
        AND ALL_MONTHS.DATA_SOURCE = BASE.DATA_SOURCE		
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID		
),
final as 
(
    select 
     cntry_cd::varchar(2) AS cntry_cd,
dim_key::varchar(32) AS dim_key,
data_source::varchar(14) AS data_source,
month::varchar(23) AS month,
so_sls_qty::numeric(38,6) AS so_sls_qty,
so_sls_value::numeric(38,6) AS so_sls_value,
so_avg_qty::numeric(38,6) AS so_avg_qty,
sales_value_list_price::numeric(38,12) AS sales_value_list_price
from wks_korea_regional_sellout_allmonths
)

--final select
select * from final

