{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["outlet_id","merchandiser_id","input_date","franchise","photo_link"],
        prehook= "delete from {{this}}
        where (outlet_id,merchandiser_id,input_date,upper(franchise),coalesce(photo_link,'na')) 
        in (select distinct trim(outlet_id),trim(merchandiser_id),cast(trim(input_date) as date),
        upper(trim(franchise)),coalesce(trim(photo_link),'na') from {{ source('idnsdl_raw', 'sdl_id_ps_promotion_competitor') }})"
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_promotion_competitor') }}
),
final as
(
    select
    trim(outlet_id) as outlet_id,
	trim(outlet_custom_code) as outlet_custom_code,
	trim(outlet_name) as outlet_name,
	trim(province) as province,
	trim(city) as city,
	trim(channel) as channel,
	trim(merchandiser_id) as merchandiser_id,
	trim(merchandiser_name) as merchandiser_name,
	trim(cust_group) as cust_group,
	trim(address) as address,
	trim(jnj_year) as jnj_year,
	trim(jnj_month) as jnj_month,
	trim(jnj_week) as jnj_week,
	trim(day_name) as day_name,
	cast(trim(input_date) as date) as input_date,
	trim(franchise) as franchise,
	trim(photo_link) as photo_link,
	file_name,
    current_timestamp()::timestamp_ntz AS crt_dttm
    from source
)
select * from final