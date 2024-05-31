{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["outlet_id","merchandiser_id","input_date","franchise","product_cmp_competitor_jnj","photo_link"],
        pre_hook="{% if is_incremental() %}
        delete from {{this}} itg
        where (itg.outlet_id,itg.merchandiser_id,itg.input_date,upper(itg.franchise),upper(itg.product_cmp_competitor_jnj),coalesce(itg.photo_link,'na'))
        in (select distinct trim(sdl.outlet_id),trim(sdl.merchandiser_id),cast(trim(sdl.input_date) as date),upper(trim(sdl.franchise)),
        upper(trim(sdl.product_cmp_competitor_jnj)),coalesce(trim(sdl.photo_link),'na') 
        from {{source('idnsdl_raw','sdl_id_ps_visibility')}} sdl);
        {% endif %}"
    )
}}

with source as
(
    select * from {{source('idnsdl_raw', 'sdl_id_ps_visibility')}}
),
final as
(
    select 
        itg.outlet_id::varchar(10) as outlet_id,
        itg.outlet_custom_code::varchar(10) as outlet_custom_code,
        itg.outlet_name::varchar(100) as outlet_name,
        itg.province::varchar(50) as province,
        itg.city::varchar(50) as city,
        itg.channel::varchar(50) as channel,
        itg.merchandiser_id::varchar(20) as merchandiser_id,
        itg.merchandiser_name::varchar(50) as merchandiser_name,
        itg.cust_group::varchar(50) as cust_group,
        itg.address::varchar(255) as address,
        itg.jnj_year::varchar(4) as jnj_year,
        itg.jnj_month::varchar(2) as jnj_month,
        itg.jnj_week::varchar(5) as jnj_week,
        itg.day_name::varchar(20) as day_name,
        itg.input_date::date as input_date,
        itg.franchise::varchar(50) as franchise,
        itg.product_cmp_competitor_jnj::varchar(50) as product_cmp_competitor_jnj,
        itg.number_of_facing::varchar(20) as number_of_facing,
        itg.share_of_shelf::number(38,5) as share_of_shelf,
        itg.photo_link::varchar(100) as photo_link,
        itg.file_name::varchar(100) as file_name,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source itg
)
select * from final
