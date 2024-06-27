{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["matl_num","acct_grp","mo","yr","ctry_cd"],
        merge_exclude_columns=["crt_dttm"]
)}}
with source as (
    select * from {{ ref('ntaitg_integration__itg_onpck_trgt') }}
),
final as (
    select
        matl_num::varchar(50) as matl_num,
        matl_desc::varchar(1000) as matl_desc,
        acct_grp::varchar(100) as acct_grp,
        mo::varchar(10) as mo,
        yr::varchar(10) as yr,
        trgt_qty::number(22,2) as trgt_qty,
        ctry_cd::varchar(20) as ctry_cd,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final