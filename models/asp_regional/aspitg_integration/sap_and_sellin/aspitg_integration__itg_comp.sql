{{
  config(
    alias="itg_comp",
    tags= [""],
    materialized="incremental",
    unique_key=["CO_CD"],
    incremental_strategy="merge",
    transient=false
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_comp') }}
),
final as
(
  SELECT
    mandt,
    bukrs,
    land1,
    waers,
    ktopl,
    kkber,
    periv,
    rcomp,
    CURRENT_TIMESTAMP() AS CRT_DTTM,
    CURRENT_TIMESTAMP() AS UPDT_DTTM
  FROM source
)
select * from final
