{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid', 'saleoffice', 'salegroup', 'target', 'period']
    )
}}

with source as(
    select * from {{ source('thasdl_raw','sdl_mds_lcm_distributor_target_sales') }}
),
final as 
(
select
    upper(trim(distributorid))::varchar(10) as distributorid,
    upper(trim(saleoffice))::varchar(200) as saleoffice,
    upper(trim(salegroup))::varchar(200) as salegroup,
    target::number(18,6) as target,
    trim(period)::varchar(10) as period,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final