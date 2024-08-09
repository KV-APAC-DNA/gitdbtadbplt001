--import cte

with wks_china_personal_care_base_retail_excellence as (
    select * from {{ ref('chnwks_integration__wks_china_personal_care_base_retail_excellence')}}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ ref('aspedw_integration__v_edw_vw_cal_Retail_excellence_dim') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_china_personal_care_regional_sellout_allmonths
as 
(
select all_months.cntry_cd,
       all_months.sellout_dim_key,       all_months.data_src,
       all_months.month,
       base.so_sls_qty,
       base.so_sls_value,
       base.so_avg_qty ,
       base.sales_value_list_price
from (select distinct cntry_cd,
             sellout_dim_key,
             data_src,
             month
      from (select distinct cntry_cd,
                   sellout_dim_key,
                   data_src
            from wks_china_personal_care_base_retail_excellence
			where nvl(so_sls_value,0) != 0 and
			(mnth_id >= (select last_28mnths from edw_vw_cal_retail_excellence_dim)
and mnth_id <= (select  last_2mnths from edw_vw_cal_retail_excellence_dim))) a,
           (select distinct "year" as "year",
                   mnth_id as month
            from edw_vw_os_time_dim
            where (mnth_id >= (select last_28mnths from edw_vw_cal_retail_excellence_dim)--::numeric
              --mnth_id > to_char(add_months(sysdate,-38),'yyyymm') 
            and mnth_id <= (select  TO_CHAR((DATEADD(DAY, 90, sysdate()::DATE )), 'YYYYMM') ))) b) all_months
  left join (select * from wks_china_personal_care_base_retail_excellence where mnth_id >= (select last_28mnths from edw_vw_cal_retail_excellence_dim)
and mnth_id <= (select  last_2mnths from edw_vw_cal_retail_excellence_dim)) base
         on all_months.cntry_cd = base.cntry_cd
        and all_months.sellout_dim_key = base.sellout_dim_key
        and all_months.data_src = base.data_src
        and all_months.month = base.mnth_id
),
final as 
(
    select cntry_cd ::varchar(2) as cntry_cd,
    sellout_dim_key :: varchar(32) as sellout_dim_key,
    data_src :: varchar(14) as data_src,
    month :: varchar(23) as month,
    so_sls_qty :: numeric(38,6) as so_sls_qty,
    so_sls_value :: numeric(38,6) as so_sls_value,
    so_avg_qty :: numeric(38,6) as so_avg_qty,
    sales_value_list_price :: numeric(38,12) as sales_value_list_price
    from wks_china_personal_care_regional_sellout_allmonths
)

select * from final
