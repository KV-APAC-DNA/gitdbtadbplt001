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
        from {{ source('idnsdl_raw', 'sdl_id_ps_pricing') }}
        where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_pricing__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_pricing__duplicate_test') }}
        )
        );
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_pricing') }}
    where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_pricing__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_pricing__duplicate_test') }}
    )
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
    trim(put_up)::varchar(100) as put_up,
    trim(competitor_variant)::varchar(50) as competitor_variant,
    trim(competitor)::varchar(100) as competitor,
    trim(price_type)::varchar(30) as price_type,
    trim(price)::varchar(30) as price,
    trim(file_name)::varchar(100) as file_name,
    sysdate() as crt_dttm
	from source
)
select * from final