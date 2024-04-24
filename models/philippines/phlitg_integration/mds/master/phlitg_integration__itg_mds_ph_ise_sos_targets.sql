{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where 0 != (select count(*) from {{source('phlsdl_raw','sdl_mds_ph_ise_sos_targets')}})"
    )
}}
with source as
(
    select * from {{source('phlsdl_raw','sdl_mds_ph_ise_sos_targets')}}
),
final as
(
    select 
        trim(name)::varchar(255) as brnd_nm,
        trim(cal_year)::varchar(50) as cal_year,
        trim(brand_id)::varchar(50) as brand_id,
        cast(trim(target) as numeric(20, 4)) as target,
        lastchgdatetime::timestamp_ntz(9) as last_chg_datetime,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source  
)
select * from final