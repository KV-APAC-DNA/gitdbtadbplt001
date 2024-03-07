{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['brnch_no']
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_mt_branch_master') }}
),
transformed as(
    select 
        account_name::varchar(20) as account,
        name::varchar(200) as branch_name,
        LTRIM(branchcode, 0)::varchar(20) as branch_code,
        branchtype_name::varchar(50) as branch_type,
        allstoretype_code::varchar(20) as allstoretype_code,
        allstoretype_name::varchar(50) as allstoretype_name,
        trim(provincecode_code)::varchar(10) as province_code,
        provincecode_name::varchar(100) as province,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
  from source
)
select * from transformed