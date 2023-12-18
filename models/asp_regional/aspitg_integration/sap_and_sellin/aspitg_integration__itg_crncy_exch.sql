{{
  config(
    alias="itg_crncy_exch",
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["clnt", "ex_rt_typ", "from_crncy", "to_crncy", "vld_from"],
    tags= ["daily"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_crncy_exch') }}
),

final as
(
  select
    clnt,
    ex_rt_typ,
    from_crncy,
    to_crncy,
    vld_from,
    ex_rt,
    from_ratio,
    to_ratio,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from source
)
select * from final
