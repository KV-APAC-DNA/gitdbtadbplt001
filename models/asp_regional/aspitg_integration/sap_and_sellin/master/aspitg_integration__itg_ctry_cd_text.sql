{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["ctry_key", "lang_key"],
    merge_exclude_columns=["crt_dttm"]
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)
select * from final
