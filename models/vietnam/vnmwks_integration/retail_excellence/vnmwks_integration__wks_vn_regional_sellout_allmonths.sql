--import cte

with wks_vn_base_retail_excellence as (
    select * from {{ ref('vnmwks_integration__wks_vn_base_retail_excellence')}}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

--final cte

wks_vn_regional_sellout_allmonths as 
(
    SELECT ALL_MONTHS.CNTRY_CD,
       ALL_MONTHS.SELLOUT_DIM_KEY,
       ALL_MONTHS.data_src,
       ALL_MONTHS.MONTH,
       BASE.SO_SLS_QTY,
       BASE.SO_SLS_VALUE,
       BASE.SO_AVG_QTY,
	   BASE.SALES_VALUE_LIST_PRICE
FROM (SELECT DISTINCT cntry_cd,
             SELLOUT_DIM_KEY,
             data_src,
             MONTH
      FROM (SELECT DISTINCT cntry_cd,
                   SELLOUT_DIM_KEY,
                   data_src
            FROM WKS_VN_BASE_RETAIL_EXCELLENCE) a,
           (SELECT DISTINCT "year",
                   mnth_id AS MONTH
            FROM EDW_VW_OS_TIME_DIM
			WHERE MNTH_ID >= (SELECT last_28mnths
                 FROM EDW_VW_CAL_RETAIL_EXCELLENCE_DIM)
                 AND  MNTH_ID <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  LEFT JOIN WKS_VN_BASE_RETAIL_EXCELLENCE base
         ON ALL_MONTHS.CNTRY_CD = BASE.CNTRY_CD
        AND ALL_MONTHS.SELLOUT_DIM_KEY = BASE.SELLOUT_DIM_KEY
        AND ALL_MONTHS.data_src = BASE.data_src
        AND ALL_MONTHS.MONTH = BASE.MNTH_ID
),

final as 
(
    select 
    cntry_cd :: varchar(2) as cntry_cd,
    sellout_dim_key :: varchar(32) as sellout_dim_key,
    data_src :: varchar(14) as data_src,
    month :: varchar(27) as month,
    so_sls_qty :: numeric(38,6) as so_sls_qty,
    so_sls_value :: numeric(38,6) as so_sls_value,
    so_avg_qty :: numeric(38,6) as so_avg_qty,
    sales_value_list_price :: numeric(38,12) as sales_value_list_price
    from wks_vn_regional_sellout_allmonths
)

--final select

select * from final