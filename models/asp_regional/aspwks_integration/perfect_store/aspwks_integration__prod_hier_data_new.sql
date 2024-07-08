with edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_perfect_store_rebase_wt_temp as (
    select * from {{ ref('aspedw_integration__edw_perfect_store_rebase_wt_temp') }}
),
fisc_per_table as (
    select country,
        dataset,
        substring(a.fisc_per, 1, 4) as fisc_yr,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag,
        min(fisc_per) as min_fisc_per,
        max(b.max_mnth_id) as max_mnth_id
    from (
            select *
            from EDW_PERFECT_STORE_REBASE_WT_temp
            WHERE kpi = 'SIZE OF THE PRIZE'
        ) a
        join (
            select substring(a.fisc_per, 1, 4) as fisc_yr,
                max(b.mnth_id) as max_mnth_id
            from (
                    select *
                    from EDW_PERFECT_STORE_REBASE_WT_temp
                    WHERE kpi = 'SIZE OF THE PRIZE'
                ) a
                join edw_vw_os_time_dim b on substring(a.fisc_per, 1, 4) = b."year"
            group by substring(a.fisc_per, 1, 4)
        ) b on substring(a.fisc_per, 1, 4) = b.fisc_yr
    group by country,
        dataset,
        substring(a.fisc_per, 1, 4),
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
    order by country,
        dataset,
        substring(a.fisc_per, 1, 4),
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
),
final as (
    select country,
        dataset,
        cast(a.mnth_id as integer) as fisc_per,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
    from (
            select distinct mnth_id
            from edw_vw_os_time_dim
            where "year" in (
                    select distinct substring(fisc_per, 1, 4)
                    from EDW_PERFECT_STORE_REBASE_WT_temp
                    WHERE kpi = 'SIZE OF THE PRIZE'
                )
        ) a
        left join fisc_per_table b on a.mnth_id >= b.min_fisc_per
        and a.mnth_id <= b.max_mnth_id
    order by country,
        dataset,
        a.mnth_id,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
)
select * from final