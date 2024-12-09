with source as (
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
)
,claimable as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_claimable') }}
)

,final as (

    select
        fisc_yr as year,
        mth_yyyymm as month,
        rtruniquecode,
        mothersku_name,
        SUM(quantity) as quantity,
        SUM(achievement_nr) as achievement_nr
    from source
    where
        fisc_yr >= 2022 and rtruniquecode in (
            select distinct rtruniquecode
            from claimable
        )
    group by 1, 2, 3, 4
)

select * from final
