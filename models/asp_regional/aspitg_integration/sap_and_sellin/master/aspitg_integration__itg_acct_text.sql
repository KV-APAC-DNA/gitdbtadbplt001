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
    chrt_accts :: varchar(4) as chrt_acct,
    account :: varchar(10) as acct_num,
    langu :: varchar(2) as lang_key,
    txtsh :: varchar(5000) as shrt_desc,
    txtmd :: varchar(5000) as med_desc,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)

select * from final