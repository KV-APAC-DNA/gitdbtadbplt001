with edw_perfect_store_rebase_wt_temp as (
    select * from snapaspwks_integration.edw_perfect_store_rebase_wt_temp
),
final as (
    SELECT *,
        --nvl(lead(compliance,1) over (partition by hashkey order by fisc_per),compliance),
        --lead(hashkey,1) over (partition by hashkey order by fisc_per),
        max(compliance) over (
            partition by hashkey
            order by fisc_per rows between unbounded preceding and current row
        ) as complaince_max
    FROM (
            SELECT dataset,
                country,
                parent_customer,
                retail_environment,
                channel,
                prod_hier_l4,
                fisc_yr,
                fisc_per,
                compliance,
                MD5(
                    nvl(dataset, 'a') || nvl(country, 'b') || nvl(parent_customer, 'c') || nvl(prod_hier_l4, 'd') || nvl(fisc_yr, 2099)
                ) as hashkey
            FROM edw_perfect_store_rebase_wt_temp
            WHERE kpi = 'SIZE OF THE PRIZE TEMP' --AND fisc_yr = 2021 and fisc_per < 202104
                --AND retail_environment = 'AU INDY PHARMACY'
            ORDER BY dataset,
                country,
                parent_customer,
                retail_environment,
                channel,
                prod_hier_l4,
                fisc_yr,
                fisc_per
        )
    ORDER BY dataset,
        country,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        fisc_yr,
        fisc_per
)
select * from final