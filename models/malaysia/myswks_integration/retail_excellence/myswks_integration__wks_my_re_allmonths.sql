--import cte     
with edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
wks_my_base_re as (
    select * from {{ ref('myswks_integration__wks_my_base_re') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),


my_re_allmonths	  as (
SELECT ALL_MONTHS.CNTRY_CD,		
       ALL_MONTHS.SELLOUT_DIM_KEY,		
       ALL_MONTHS.MONTH,		
       BASE.SO_SLS_QTY::NUMERIC(38,6) AS SO_SLS_QTY,		
       BASE.SO_SLS_VALUE::NUMERIC(38,6) AS SO_SLS_VALUE,		
       BASE.SO_AVG_QTY::NUMERIC(38,6) AS SO_AVG_QTY,		
       BASE.SALES_VALUE_LIST_PRICE		
FROM (SELECT DISTINCT cntry_cd,
             sellout_dim_key,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   sellout_dim_key
            FROM WKS_MY_BASE_RE where MNTH_ID >= (select last_28mnths from edw_vw_cal_Retail_excellence_Dim)::numeric
	   and mnth_id <= (select last_2mnths from edw_vw_cal_Retail_excellence_Dim)::numeric) a,
	    (select distinct "year",
                   mnth_id as month
            from edw_vw_os_time_dim		
            where mnth_id >= (select last_28mnths
                 from edw_vw_cal_retail_excellence_dim)		
 and   mnth_id <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  LEFT JOIN (SELECT * FROM WKS_MY_BASE_RE where MNTH_ID >= (select last_28mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::numeric		
	   and mnth_id <= (select last_2mnths from EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)::numeric) base		
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD		
        AND ALL_MONTHS.SELLOUT_DIM_KEY = BASE.SELLOUT_DIM_KEY		
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID
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

 from my_re_allmonths
)
select * from final