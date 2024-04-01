{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["prvnce_cd"],
        pre_hook= "delete from {{this}} where coalesce(ltrim(prvnce_cd,0),'NA') in (select coalesce(ltrim(code,0),'NA') from {{ source('thasdl_raw', 'sdl_mds_th_ref_city') }})"
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_city') }}
),
transformed as(
    select 
        code::varchar(50) as prvnce_cd,
        name::varchar(100) as prvnce_nm,
        cityenglish::varchar(100) as prvnce_eng_nm,
        region_name::varchar(50) as region,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from transformed