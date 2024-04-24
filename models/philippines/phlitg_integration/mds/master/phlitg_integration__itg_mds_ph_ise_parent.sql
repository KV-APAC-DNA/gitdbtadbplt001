{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where 0 != (select count(*) from {{source('phlsdl_raw','sdl_mds_ph_ise_parent')}})"
    )
}}
with source as
(
    select * from {{source('phlsdl_raw','sdl_mds_ph_ise_parent')}}
),
final as
(
    select 
        trim(code)::varchar(50) as classid,
        trim(name)::varchar(255) as descr,
        trim(parentcustomercode_code)::varchar(50) as parentcustomercode,
        trim(parentcustomercode_name)::varchar(255) as parentcustomername,
        trim(soldtocode_code)::varchar(50) as soldtocode,
        trim(soldtocode_name)::varchar(255) as soldtoname,
        trim(mtprefix)::varchar(50) as mtprefix,
        lastchgdatetime::timestamp_ntz(9) as last_chg_datetime,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source  
)
select * from final