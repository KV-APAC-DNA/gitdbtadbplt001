with edw_crncy_exch as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),

final as (
    select
        clnt as "clnt",
        ex_rt_typ as "ex_rt_typ",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        vld_from as "vld_from",
        ex_rt as "ex_rt",
        from_ratio as "from_ratio",
        to_ratio as "to_ratio",
        crt_dttm as "crt_dttm",
        updt_dttm as "updt_dttm"

    from edw_crncy_exch
)

select * from final
