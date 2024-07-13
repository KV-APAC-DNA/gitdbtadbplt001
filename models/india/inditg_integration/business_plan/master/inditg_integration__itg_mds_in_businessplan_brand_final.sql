{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}}
        where  (year,month) in (select distinct year_code,month_code
        from {{ ref('inditg_integration__itg_mds_in_businessplan_brand') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ ref('inditg_integration__itg_mds_in_businessplan_brand') }}
),
final as 
(
    select 
    year_code::varchar(500) as year,
	month_code::varchar(500) as month,
	gsm_code::varchar(500) as gsm,
	region_code::varchar(500) as region,
	variant_tableau_code::varchar(500) as variant_tableau,
	variant_name_code::varchar(500) as variant_name,
	franchise_dsr_code::varchar(500) as franchise_dsr,
	brand_dsr_code::varchar(500) as brand_dsr,
	business_plan::number(31,5) as business_plan,
	year_code||month_code::varchar(500) as date
    from source
)
select  * from final