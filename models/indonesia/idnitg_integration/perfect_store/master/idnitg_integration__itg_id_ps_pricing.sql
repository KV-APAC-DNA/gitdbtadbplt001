{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["outlet_id","merchandiser_id","input_date","put_up","competitor","price_type"],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (outlet_id,merchandiser_id,input_date,upper(put_up),
        upper(competitor),upper(price_type)) in (select distinct trim(outlet_id),
        trim(merchandiser_id),cast(trim(input_date) as date),upper(trim(put_up)),
        upper(trim(competitor)),upper(trim(price_type)) 
        from {{ source('idnsdl_raw', 'sdl_id_ps_pricing') }});
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_pricing') }}
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
	trim(put_up) as put_up,
	trim(competitor_variant) as competitor_variant,
	trim(competitor) as competitor,
	trim(price_type) as price_type,
	trim(price) as price,
	file_name,
    current_timestamp()::timestamp_ntz as crt_dttm
	from source
)
select * from final