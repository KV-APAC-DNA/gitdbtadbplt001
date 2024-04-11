{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where (select count(*) from {{source('pcfsdl_raw','sdl_mds_pacific_ps_targets')}}) >0 "
    )
}}
with source as
(
    select * from {{source('pcfsdl_raw','sdl_mds_pacific_ps_targets')}}
),
final as
(
    select 
        name::varchar(100) as name,
        channel::varchar(100) as channel,
        re_customer_flag::varchar(100) as re_customer_flag,
        kpi::varchar(100) as kpi,
        attribute_1::varchar(100) as attribute_1,
        attribute_2::varchar(100) as attribute_2,
        value::number(20,4) as value,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        valid_from::timestamp_ntz(9) as valid_from,
        valid_to::timestamp_ntz(9) as valid_to
    from source    
)
select * from final