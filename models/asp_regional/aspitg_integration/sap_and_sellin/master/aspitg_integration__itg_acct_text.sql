{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["chrt_acct","acct_num","lang_key"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_acct_text') }}
),

final as (
    select
    chrt_accts as chrt_acct,
    account as acct_num,
    langu as lang_key,
    txtsh as shrt_desc,
    txtmd as med_desc,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final