{{
  config(
    alias="itg_comp",
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["co_cd"],
    tags= ["daily"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_comp') }}
),

final as
(
  select
    mandt as clnt,
    bukrs as co_cd,
    land1 as ctry_key,
    waers as crncy_key,
    ktopl as chrt_acct,
    kkber as crdt_cntrl_area,
    periv as fisc_yr_vrnt,
    rcomp as company,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)
select * from final
