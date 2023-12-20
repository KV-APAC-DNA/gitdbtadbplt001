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

with itg_crncy_exch as (
    select * from {{ ref('aspitg_integration__itg_crncy_exch') }}
),
    
itg_crncy_conv as (
    select * from {{ source('aspitg_integration', 'itg_crncy_conv') }}
),

itg_crncy_conv_filtered as ( 
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
),

transformed as (
    select
        itg_crncy_exch.clnt,
        itg_crncy_exch.ex_rt_typ,
        itg_crncy_exch.from_crncy,
        itg_crncy_exch.to_crncy,
        itg_crncy_exch.vld_from,
        itg_crncy_exch.ex_rt,
        case when itg_crncy_exch.ex_rt_typ = 'BWAR' then itg_crncy_conv_filtered.from_ratio else itg_crncy_exch.from_ratio end as from_ratio,
        case when itg_crncy_exch.ex_rt_typ = 'BWAR' then itg_crncy_conv_filtered.to_ratio else itg_crncy_exch.to_ratio end as to_ratio,
        itg_crncy_exch.updt_dttm
    from itg_crncy_exch
    left outer join itg_crncy_conv_filtered
      on itg_crncy_exch.ex_rt_typ = itg_crncy_conv_filtered.ex_rt_typ and itg_crncy_exch.from_crncy = itg_crncy_conv_filtered.from_crncy and itg_crncy_exch.to_crncy = itg_crncy_conv_filtered.to_crncy
),

final as (
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
    from transformed
)

select * from final
