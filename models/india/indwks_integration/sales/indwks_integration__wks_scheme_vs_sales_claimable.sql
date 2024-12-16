with source as (
    select * from {{ ref('indedw_integration__edw_rpt_schemeutilize_cube') }}
),

final as (

    select
        fisc_yr as year,
        mth_yyyymm as month,
        rtruniquecode,
        mothersku_name,
        franchise_name,
        brand_name,
        variant_name,
        SUM(schemeutilizedqty) as schemeutilizedqty,
        SUM(schemeutilizedamt) as schemeutilizedamt
    from source
    where claimable = 'Yes'
    group by 1, 2, 3, 4, 5, 6, 7
)

select * from final
