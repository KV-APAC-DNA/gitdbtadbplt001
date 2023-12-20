{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["lang_key","cntl_area","prft_ctr","vld_to_dt","vld_from_dt"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_prft_ctr_text') }}
),

final as (
    select
        langu as lang_key,
        kokrs as cntl_area,
        prctr as prft_ctr,
        dateto as vld_to_dt,
        datefrom as vld_from_dt,
        txtsh as shrt_desc,
        txtmd as med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  FROM source
)

select * from final