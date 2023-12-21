{{
  config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["cust_num1"],
        merge_exclude_columns=["crt_dttm"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_cust_text') }}
),

final as
(
    select
        mandt as clnt,
        kunnr as cust_num1,
        txtmd as nm,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
