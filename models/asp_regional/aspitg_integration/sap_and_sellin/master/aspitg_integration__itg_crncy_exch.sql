{{
  config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["clnt", "ex_rt_typ", "from_crncy", "to_crncy", "vld_from"],
        merge_exclude_columns=["crt_dttm"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_crncy_exch') }}
),

trans as
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
),

final as(
    select 
        clnt::number(18,0) as clnt,
        ex_rt_typ::varchar(4) as ex_rt_typ,
        from_crncy::varchar(5) as from_crncy,
        to_crncy::varchar(5) as to_crncy,
        vld_from::varchar(8) as vld_from,
        ex_rt::number(9,5) as ex_rt,
        from_ratio::number(9,0) as from_ratio,
        to_ratio::number(9,0) as to_ratio,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
