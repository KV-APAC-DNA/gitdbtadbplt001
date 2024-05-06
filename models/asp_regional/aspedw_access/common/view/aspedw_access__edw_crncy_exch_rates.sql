with edw_crncy_exch_rates as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),

final as (
    select
        valid_from as "valid_from",
        fisc_yr_per as "fisc_yr_per",
        fisc_yr_per_rep as "fisc_yr_per_rep",
        fisc_yr_per_fday as "fisc_yr_per_fday",
        fisc_yr_per_lday as "fisc_yr_per_lday",
        ex_rt_typ as "ex_rt_typ",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        act_valid_from as "act_valid_from",
        ex_rt as "ex_rt",
        from_ratio as "from_ratio",
        to_ratio as "to_ratio"

    from edw_crncy_exch_rates
)

select * from final
