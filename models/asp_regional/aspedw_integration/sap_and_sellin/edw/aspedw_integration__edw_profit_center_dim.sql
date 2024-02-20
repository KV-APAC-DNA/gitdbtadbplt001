{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["prft_ctr"],
        merge_exclude_columns = ["crt_dttm","rflt","strng_hold_shrt_desc","need_stat_shrt_desc"]
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
        lang_key::varchar(4) as lang_key,
        cntl_area::varchar(10) as cntl_area,
        prft_ctr::varchar(40) as prft_ctr,
        vld_to_dt::date as vld_to_dt,
        vld_from_dt::date as vld_from_dt,
        shrt_desc::varchar(20) as shrt_desc,
        med_desc::varchar(40) as med_desc,
        prsn_resp::varchar(20) as prsn_resp,
        crncy_key::varchar(5) as crncy_key,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        null::varchar(100) as need_stat_shrt_desc,
        null::varchar(100) as strng_hold_shrt_desc,
        null::varchar(100) as rflt
  from source
)

select * from final