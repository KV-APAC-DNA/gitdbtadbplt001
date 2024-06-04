{{
    config(
        materialized='incremental',
        incremental_strategy= "append",
        unique_key= ["outlet_id","merchandiser_id","input_date","franchise","photo_link"],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (outlet_id,merchandiser_id,input_date,upper(franchise),
        coalesce(photo_link,'na')) in (select distinct trim(outlet_id),trim(merchandiser_id),
        cast(trim(input_date) as date),upper(trim(franchise)),coalesce(trim(photo_link),'na')
         from {{ source('idnsdl_raw', 'sdl_id_ps_promotion_competitor') }});
         {% endif %}"
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_promotion_competitor') }}
),
final as
(
    select
    trim(outlet_id)::varchar(10) as outlet_id,
    trim(outlet_custom_code)::varchar(10) as outlet_custom_code,
    trim(outlet_name)::varchar(100) as outlet_name,
    trim(province)::varchar(50) as province,
    trim(city)::varchar(50) as city,
    trim(channel)::varchar(50) as channel,
    trim(merchandiser_id)::varchar(20) as merchandiser_id,
    trim(merchandiser_name)::varchar(50) as merchandiser_name,
    trim(cust_group)::varchar(50) as cust_group,
    trim(address)::varchar(255) as address,
    trim(jnj_year)::varchar(4) as jnj_year,
    trim(jnj_month)::varchar(2) as jnj_month,
    trim(jnj_week)::varchar(5) as jnj_week,
    trim(day_name)::varchar(20) as day_name,
    cast(trim(input_date) as date) as input_date,
    trim(franchise)::varchar(50) as franchise,
    trim(photo_link)::varchar(100) as photo_link,
    trim(description)::varchar(1000) as description,
    trim(file_name)::varchar(100) as file_name,
    sysdate() as crt_dttm
    from source
)
select * from final