{{
    config(
        alias="itg_prft_ctr",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cntl_area","prft_ctr","vld_to_dt","vld_from_dt"],
        merge_exclude_columns = ["crt_dttm","strng_hold","need_stat"],
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_prft_ctr') }}
),

final as (
    select
    kokrs as cntl_area,
    prctr as prft_ctr,
    dateto as vld_to_dt,
    datefrom as vld_from_dt,
    verak as prsn_resp,
    waers as crncy_key,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    null as strng_hold,
    null as need_stat
  FROM source
)

select * from final