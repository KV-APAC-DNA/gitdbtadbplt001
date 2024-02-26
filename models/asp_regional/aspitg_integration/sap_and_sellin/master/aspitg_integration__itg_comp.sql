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
  select * from {{ ref('aspwks_integration__wks_itg_comp') }}
),

trans as
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
        current_timestamp() as crt_dttm,
        current_timestamp() as updt_dttm
    from source
),

final as(
    select
        clnt::varchar(3) as clnt,
        co_cd::varchar(4) as co_cd,
        ctry_key::varchar(3) as ctry_key,
        crncy_key::varchar(5) as crncy_key,
        chrt_acct::varchar(4) as chrt_acct,
        crdt_cntrl_area::varchar(4) as crdt_cntrl_area,
        fisc_yr_vrnt::varchar(2) as fisc_yr_vrnt,
        company::varchar(6) as company,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
