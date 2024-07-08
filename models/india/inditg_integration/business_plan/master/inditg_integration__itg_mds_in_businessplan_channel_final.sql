{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        where  (year,month1) in (select distinct year_code,month_code
        from {{ ref('inditg_integration__itg_mds_in_businessplan_channel') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_channel') }}
),
final as 
(
    select 
    region_code::varchar(500) as region,
	gsm_code::varchar(500) as gsm,
	channel_name_code::varchar(500) as channel_name,
	Plan::number(31,5) as plan,
	year_code::varchar(500) as year,
	to_char(to_date(month_code, 'MON'), 'MM')::varchar(500) as month,
	month_code::varchar(500) as month1
    from source
)
select  * from final