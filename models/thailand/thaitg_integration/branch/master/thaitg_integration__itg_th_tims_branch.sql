{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["brnch_no"],
        pre_hook= "delete from {{this}} where coalesce(ltrim(brnch_no,0),'NA') in (select coalesce(ltrim(branchcode,0),'NA') from {{ source('thasdl_raw', 'sdl_mds_th_mt_branch_master') }})"
    )
}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_mt_branch_master') }}
),
transformed as(
    select
        branchcode::varchar(100) as brnch_no,
        name::varchar(100) as branch_nm,
        branchtype_name::varchar(100) as brnch_typ,
        allstoretype_name::varchar(100) as all_str_typ,
        provincecode_code::varchar(50) as prvnce_cd,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
   from source WHERE UPPER(account_name) IN ('LOTUS')
)
select * from transformed