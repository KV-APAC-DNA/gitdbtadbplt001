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

trans as
(
  select
        country as ctry_key,
        langu as lang_key,
        txtsh as shrt_desc,
        txtmd as med_desc,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
),
final as(
    select
        ctry_key::varchar(3) as ctry_key,
        lang_key::varchar(1) as lang_key,
        shrt_desc::varchar(20) as shrt_desc,
        med_desc::varchar(40) as med_desc,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
