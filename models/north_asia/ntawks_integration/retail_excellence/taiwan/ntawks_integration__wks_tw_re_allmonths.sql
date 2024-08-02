--import cte     
with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_tw_base_re as (
    select * from {{ ref('ntawks_integration__wks_tw_base_re') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),


tw_re_allmonths	  as (
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
            FROM WKS_TW_BASE_RE) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM EDW_VW_OS_TIME_DIM		--//             FROM os_edw.edw_vw_os_time_dim
			WHERE MNTH_ID >= (SELECT last_28mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)		
AND   MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  LEFT JOIN WKS_TW_BASE_RE base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.MONTH = base.MNTH_ID
),

--final select
final as (
    select
cntry_cd::varchar(2) as cntry_cd ,
sellout_dim_key::varchar(32) as sellout_dim_key ,
month::varchar(27) as month ,
so_sls_qty::numeric(38,6) as so_sls_qty ,
so_sls_value::numeric(38,6) as so_sls_value ,
so_avg_qty::numeric(38,6) as so_avg_qty ,
sales_value_list_price::numeric(38,12) as sales_value_list_price 

 from tw_re_allmonths
)
select * from final