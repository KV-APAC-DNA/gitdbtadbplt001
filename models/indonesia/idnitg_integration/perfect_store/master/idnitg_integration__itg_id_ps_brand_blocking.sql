{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["outlet_id","merchandiser_id","input_date","franchise","photo_link"],
        pre_hook="{% if is_incremental() %}
        delete from {{this}} itg
        where (itg.outlet_id, itg.merchandiser_id, itg.input_date, upper(itg.franchise), coalesce(itg.photo_link, 'NA')) 
        in (select distinct trim(sdl.outlet_id), trim(sdl.merchandiser_id), to_date(trim(sdl.input_date)), upper(trim(sdl.franchise)), coalesce(trim(sdl.photo_link), 'NA') 
        from {{source('idnsdl_raw', 'sdl_id_ps_brand_blocking')}} SDL
        where SDL.file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_brand_blocking__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_brand_blocking__duplicate_test') }}
        )
        );
        {% endif %}"
    )
}}

with source as 
(
    select *, dense_rank() over(partition by trim(outlet_id), trim(merchandiser_id), to_date(trim(input_date)), upper(trim(franchise)), coalesce(trim(photo_link), 'NA') order by file_name desc) as rnk 
    from  {{source('idnsdl_raw', 'sdl_id_ps_brand_blocking')}}
    where file_name not in (
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_brand_blocking__null_test') }}
            union all
            select distinct file_name from {{ source('idnwks_integration', 'TRATBL_sdl_id_ps_brand_blocking__duplicate_test') }}
    ) qualify rnk =1
),
final as
(
    select 
        trim(sdl.outlet_id)::varchar(10) as outlet_id,
        trim(sdl.outlet_custom_code)::varchar(10) as outlet_custom_code,
        trim(sdl.outlet_name)::varchar(100) as outlet_name,
        trim(sdl.province)::varchar(50) as province,
        trim(sdl.city)::varchar(50) as city,
        trim(sdl.channel)::varchar(50) as channel,
        trim(sdl.merchandiser_id)::varchar(20) as merchandiser_id,
        trim(sdl.merchandiser_name)::varchar(50) as merchandiser_name,
        trim(sdl.cust_group)::varchar(50) as cust_group,
        trim(sdl.address)::varchar(255) as address,
        trim(sdl.jnj_year)::varchar(4) as jnj_year,
        trim(sdl.jnj_month)::varchar(2) as jnj_month,
        trim(sdl.jnj_week)::varchar(5) as jnj_week,
        trim(sdl.day_name)::varchar(20) as day_name,
        cast(trim(sdl.input_date) as date) as input_date,
        trim(sdl.franchise)::varchar(50) as franchise,
        trim(sdl.photo_link)::varchar(100) as photo_link,
        trim(sdl.file_name)::varchar(100) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source sdl
)
select * from final





