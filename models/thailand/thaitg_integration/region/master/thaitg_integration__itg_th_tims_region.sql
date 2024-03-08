{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['prvnce_cd_pk']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ref_city') }}
),
transformed as(
    select 
        code::varchar(50) as prvnce_cd,
        COALESCE(LTRIM(code, 0), 'NA')::varchar(50) as prvnce_cd_pk,
        name::varchar(100) as prvnce_nm,
        cityenglish::varchar(100) as prvnce_eng_nm,
        region_name::varchar(50) as region,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from transformed