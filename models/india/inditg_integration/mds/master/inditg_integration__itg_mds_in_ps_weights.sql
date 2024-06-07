{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where 0 !=  ( select count(*) from {{ source('indsdl_raw', 'sdl_mds_in_ps_weights') }} );
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_ps_weights') }}
),
final as 
(
    select
        kpi::varchar(100) as kpi,
        channel::varchar(100) as channel,
        retail_env::varchar(100) as retail_env,
        weight::number(20,4) as weight,
        convert_timezone('UTC', current_timestamp()) AS crtd_dttm,
    from source
)
select * from final

