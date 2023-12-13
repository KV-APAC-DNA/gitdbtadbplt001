{{
  config(
    alias="itg_cust_text",
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["cust_num1"],
    tags= [""]
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
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
  from source
)
select * from final
