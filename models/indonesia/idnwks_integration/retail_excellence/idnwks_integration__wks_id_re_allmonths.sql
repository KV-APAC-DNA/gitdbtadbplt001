--import cte
with wks_id_base_re as(
    select * from {{ ref('idnwks_integration__wks_id_base_re') }}
),

edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

v_edw_vw_cal_Retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),

--final cte
wks_id_re_allmonths as (
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
            FROM wks_id_base_re 
            where  MNTH_ID >= (select last_36mnths from v_edw_vw_cal_retail_excellence_dim)::numeric
	        and mnth_id <= (select prev_mnth from v_edw_vw_cal_retail_excellence_dim)::numeric) a,
           (SELECT DISTINCT "year" AS "YEAR",
                   mnth_id AS "MONTH"
            FROM edw_vw_os_time_dim
			where MNTH_ID >= (select last_36mnths from v_edw_vw_cal_retail_excellence_dim)
	         AND   mnth_id <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )
			) b) all_months
  LEFT JOIN (select * from wks_id_base_re 
               where  MNTH_ID >= (select last_36mnths from v_edw_vw_cal_retail_excellence_dim)::numeric
	           and mnth_id <= (select prev_mnth from v_edw_vw_cal_retail_excellence_dim)::numeric ) base
         ON all_months.cntry_cd = base.cntry_cd
        AND all_months.sellout_dim_key = base.sellout_dim_key
        AND all_months.MONTH = base.MNTH_ID
),

final as (
    select 
    cntry_cd::varchar(2) as cntry_cd,
    sellout_dim_key::varchar(32) as sellout_dim_key,
    month::varchar(23) as month,
    so_sls_qty::numeric(38,6) as so_sls_qty,
    so_sls_value::numeric(38,6) as so_sls_value,
    so_avg_qty::numeric(38,6) as so_avg_qty,
    sales_value_list_price::numeric(38,12) as sales_value_list_price
    from wks_id_re_allmonths
)

--final select
select * from final