{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where (select count(*) from {{source('pcfsdl_raw','sdl_mds_pacific_ps_weights')}}) >0 "
    )
}}
with source as
(
    select * from {{source('pcfsdl_raw','sdl_mds_pacific_ps_weights')}}
),
final as
(
    select 
        ctry::varchar(510) as market,
        channel::varchar(100) as channel,
        retail_environment::varchar(100) as retail_env,
        kpi_name::varchar(100) as kpi,
        value::number(20,4) as weight,
        current_timestamp::timestamp_ntz(9) as crtd_dttm
    from source    
)
select * from final