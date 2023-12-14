{{
    config(
        alias="edw_profit_center_dim",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["prft_ctr"],
        merge_exclude_columns = ["crt_dttm"],
        tags=[""]
    )
}}
-- LTRIM(edw_profit_center_dim.prft_ctr, 0) = LTRIM(WKS_edw_profit_center_dim.prft_ctr, 0)
-- unique_key=["cntl_area","prft_ctr","vld_to_dt"],

with 

source as (

    select * from {{ ref('aspwks_integration__wks_edw_profit_center_dim') }}
),

final as (
    select
    lang_key,
    cntl_area,
    prft_ctr,
    vld_to_dt,
    vld_from_dt,
    shrt_desc,
    med_desc,
    prsn_resp,
    crncy_key,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm,
    null as need_stat_shrt_desc,
    null as strng_hold_shrt_desc,
    null as rflt
  from source
)

select * from final