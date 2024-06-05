--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}

with edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_singapore_base_retail_excellence as (
    select * from {{ ref('sgpwks_integration__wks_singapore_base_retail_excellence') }}
),
edw_vw_os_time_dim as (
    select * from {{ source('sgpedw_integration','edw_vw_os_time_dim') }}
),
--final cte
singapore_regional_sellout_allmonths  as (
select all_months.cntry_cd,		
       all_months.sellout_dim_key,		
       all_months.month,		
       base.so_sls_qty,		
       base.so_sls_value,		
       base.so_avg_qty,		
       base.sales_value_list_price		
from (select distinct cntry_cd,
             sellout_dim_key,
             month
      from (select distinct cntry_cd,
                   sellout_dim_key
            from wks_singapore_base_retail_excellence where mnth_id >= (select last_36mnths		
                        from edw_vw_cal_retail_excellence_dim)::numeric		--//                        
      and   mnth_id <= (select prev_mnth from edw_vw_cal_retail_excellence_dim)::numeric) a,		
           (select distinct "year",
                   mnth_id as month
            from edw_vw_os_time_dim		
            where mnth_id >= (select last_36mnths
                 from edw_vw_cal_retail_excellence_dim)		
 and   mnth_id <= (select TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') )) b) all_months
  left join (select * from wks_singapore_base_retail_excellence where mnth_id >= (select last_36mnths		
                        from edw_vw_cal_retail_excellence_dim)::numeric		--//                         
      and   mnth_id <= (select prev_mnth from edw_vw_cal_retail_excellence_dim)::numeric) base		
         on all_months.cntry_cd = base.cntry_cd		
        and all_months.sellout_dim_key = base.sellout_dim_key		
        and all_months.month = base.mnth_id		
)

--final select
select * from singapore_regional_sellout_allmonths

