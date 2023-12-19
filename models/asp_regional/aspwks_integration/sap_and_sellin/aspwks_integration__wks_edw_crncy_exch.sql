{{
    config(
        alias="wks_edw_crncy_exch",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["clnt","vld_from","ex_rt_typ", "from_crncy", "to_crncy"],
        merge_exclude_columns=["crt_dttm"],
        tags=["daily"]
    )
}}
with
    itg_crncy_exch as 
        (select * from {{ ref('aspitg_integration__itg_crncy_exch') }}
        ),
    itg_crncy_conv as 
        (select * from {{ ref('aspitg_integration__itg_crncy_conv') }}
        ),
    final as (
        select
    src.clnt ,
    src.ex_rt_typ,
    src.from_crncy,
    src.to_crncy,
    src.vld_from,
    src.ex_rt,
    from_ratio,
    to_ratio,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  from (
    select
      a.clnt,
      a.ex_rt_typ,
      a.from_crncy,
      a.to_crncy,
      a.vld_from,
      a.ex_rt,
      case when a.ex_rt_typ = 'bwar' then b.from_ratio else a.from_ratio end as from_ratio,
      case when a.ex_rt_typ = 'bwar' then b.to_ratio else a.to_ratio end as to_ratio,
      a.updt_dttm
    from itg_crncy_exch as a
    left outer join (
      select
        *
      from (
        select
          ex_rt_typ,
          from_crncy,
          to_crncy,
          from_ratio,
          to_ratio,
          rank() over (partition by ex_rt_typ, from_crncy, to_crncy order by vld_from asc) as rnk
        from itg_crncy_conv
      )
      where
        rnk = 1
    ) as b
      on a.ex_rt_typ = b.ex_rt_typ and a.from_crncy = b.from_crncy and a.to_crncy = b.to_crncy
  ) as src
    )

select * from final
