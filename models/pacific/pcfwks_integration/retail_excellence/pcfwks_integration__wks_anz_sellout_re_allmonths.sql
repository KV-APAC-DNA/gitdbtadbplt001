with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_anz_sellout_base_retail_excellence as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_base_retail_excellence') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
--final cte
anz_sellout_re_sellout_allmonths  as (
SELECT all_months.cntry_cd,
       all_months.sellout_dim_key,
       all_months.month,
       base.SO_SLS_QTY::NUMERIC(38,6) AS SO_SLS_QTY,
       base.SO_SLS_VALUE::NUMERIC(38,6) AS SO_SLS_VALUE,
       base.SO_AVG_QTY::NUMERIC(38,6) AS SO_AVG_QTY,
       BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key
            FROM wks_anz_sellout_base_retail_excellence where  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) a,
           (SELECT DISTINCT "year" AS year,
                   mnth_id AS MONTH
            FROM edw_vw_os_time_dim
			where MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)
	                    AND   mnth_id <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )
			) b) all_months
  LEFT JOIN (select * from wks_anz_sellout_base_retail_excellence where  MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	  and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric )base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.MONTH = base.MNTH_ID
),
final as 
(
    select 
     cntry_cd::VARCHAR(2) AS cntry_cd,
sellout_dim_key::VARCHAR(32) AS sellout_dim_key,
month::VARCHAR(23) AS month,
so_sls_qty::NUMERIC(38,6) AS so_sls_qty,
so_sls_value::NUMERIC(38,6) AS so_sls_value,
so_avg_qty::NUMERIC(38,6) AS so_avg_qty,
sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price
from anz_sellout_re_sellout_allmonths
)

--final select
select * from final

