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

trans as
(
    select
        mandt as clnt,
        bukrs as co_cd,
        txtmd as com_cd_nm,
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm
    from source
),

final as(
    select 
        clnt::varchar(3) as clnt,
        co_cd::varchar(4) as co_cd,
        com_cd_nm::varchar(25) as com_cd_nm,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
