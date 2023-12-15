{{
  config(
    alias="itg_ctry_cd_text",
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["ctry_key", "lang_key"],
    tags= ["daily"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_country_code_text') }}
),

final as
(
  select
    country as ctry_key,
    langu as lang_key,
    txtsh as shrt_desc,
    txtmd as med_desc,
    current_timestamp() as crt_dttm,
    current_timestamp() as updt_dttm
  from source
)
select * from final
