{{
  config(
    alias="itg_comp_text",
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["co_cd"],
    tags= [""]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_company_code_text') }}
),

final as
(
  select
    mandt as clnt,
    bukrs as co_cd,
    txtmd as com_cd_nm,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
  from source
)
select * from final
