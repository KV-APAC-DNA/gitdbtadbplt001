{{
  config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["co_cd"],
        merge_exclude_columns=["crt_dttm"]
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
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final
