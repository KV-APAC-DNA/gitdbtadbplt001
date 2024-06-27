{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["matl_num","acct_grp","mo","yr","ctry_cd"],
        merge_exclude_columns=["crt_dttm"]
)}}
with source as (
    select * from {{ source('ntasdl_raw','sdl_na_onpack_target') }}
),
final as (
    select
        material_number::varchar(50) as matl_num,
        description::varchar(1000) as matl_desc,
        account_group::varchar(100) as acct_grp,
        month::varchar(10) as mo,
        year::varchar(10) as yr,
        target_quantity::number(22,2) as trgt_qty,
        country_code::varchar(20) as ctry_cd,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final