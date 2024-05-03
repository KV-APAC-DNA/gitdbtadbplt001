{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where 0 != (select count(*) from {{source('phlsdl_raw','sdl_mds_ph_ise_weights')}})"
    )
}}
with source as
(
    select * from {{source('phlsdl_raw','sdl_mds_ph_ise_weights')}}
),
final as
(
    select 
        trim(name)::varchar(50) as iseid,
        trim(kpi)::varchar(255) as kpi,
        cast(trim(weight) as numeric(20, 4)) as weight,
        lastchgdatetime::timestamp_ntz(9) as last_chg_datetime,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source  
)
select * from final