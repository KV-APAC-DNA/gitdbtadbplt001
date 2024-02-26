{{
  config(
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

trans as
(
    select
        mandt as clnt,
        kunnr as cust_num1,
        txtmd as nm,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm
    from source
),

final as(
    select 
        clnt::varchar(3) as clnt,
        cust_num1::varchar(10) as cust_num1,
        nm::varchar(100) as nm,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)

select * from final
