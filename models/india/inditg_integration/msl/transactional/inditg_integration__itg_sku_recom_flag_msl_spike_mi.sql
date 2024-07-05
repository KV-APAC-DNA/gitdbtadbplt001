{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (year,quarter) in (select fisc_yr,qtr 
                         from {{ ref('indwks_integration__wks_skurecom_mi_target_tmp3') }}
                         group by 1,2)
        {% endif %}"
    )
}}
with wks_skurecom_mi_target_tmp3 as (
    select * from {{ ref('indwks_integration__wks_skurecom_mi_target_tmp3') }}
),
final as (
    select 
    fisc_yr::number(18,0) as year,
	qtr::number(18,0) as quarter,
	mothersku_code::varchar(50) as mother_sku_cd,
	null::varchar(50) as dist_outlet_cd,
	customer_code::varchar(50) as cust_cd,
	0::varchar(50) as oos_flag,
	1::varchar(50) as ms_flag,
	0::varchar(50) as cs_flag,
	1::varchar(50) as soq,
	null::varchar(50) as unique_ret_cd,
	retailer_code::varchar(50) as retailer_cd,
	null::varchar(50) as route_code,
	null::varchar(50) as salesman_code,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from wks_skurecom_mi_target_tmp3
)
select * from final