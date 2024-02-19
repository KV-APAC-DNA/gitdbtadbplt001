{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cntl_area","prft_ctr","vld_to_dt","vld_from_dt"],
        merge_exclude_columns = ["crt_dttm","strng_hold","need_stat"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_prft_ctr') }}
),

trans as (
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
  from source
),
final as(
    select
        cntl_area::varchar(4) as cntl_area,
        prft_ctr::varchar(10) as prft_ctr,
        vld_to_dt::date as vld_to_dt,
        vld_from_dt::date as vld_from_dt,
        prsn_resp::varchar(20) as prsn_resp,
        crncy_key::varchar(5) as crncy_key,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm,
        strng_hold::number(18,0) as strng_hold,
        need_stat::number(18,0) as need_stat
    from trans
)

select * from final