{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        unique_key = ["year_code","month_code"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        where  (year,month1) in (select distinct year_code,month_code
        from {{ ref('inditg_integration__itg_mds_in_businessplan_zonewise') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_zonewise') }}
),
final as 
(
    select 
    zone_name_code::varchar(500) as zone_name,
	region_name_code::varchar(500) as region_name,
	gsm_code::varchar(500) as gsm,
	inv_business_plan::number(31,7) as inv_business_plan,
	ret_business_plan::number(31,7) as ret_business_plan,
	plan_name_code::varchar(500) as plan_name,
	year_code::varchar(500) as year,
	to_char(to_date(month_code, 'MON'), 'MM')::varchar(500) as month,
	month_code::varchar(10) as month1
    from source
)
select  * from final