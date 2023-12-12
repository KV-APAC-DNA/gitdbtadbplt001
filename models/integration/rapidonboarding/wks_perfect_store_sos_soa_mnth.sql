{{
    config(
        materialized='view'
    )
}}

--Import CTE
with rg_wks_wks_edw_perfect_store_hash as (
    select *
    from {{ ref('stg_arsadpprd001_raw__rg_wks_wks_edw_perfect_store_hash') }}
),

--Logical CTE

--Final CTE
final as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
    from rg_wks_wks_edw_perfect_store_hash
    where
        kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
        and mkt_share is not null
        and ques_type in ('DENOMINATOR', 'NUMERATOR')
        and REGEXP_COUNT(actual_value, '^[0.00-9]+$') > 0
        --   and country = 'Taiwan'
        and (country, customername, scheduleddate, prod_hier_l4, prod_hier_l5)
        in
        (
            select
                country,
                customername,
                scheduleddate,
                prod_hier_l4,
                prod_hier_l5
            from rg_wks_wks_edw_perfect_store_hash
            where
                kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
                and prod_hier_l4 is not null
                and prod_hier_l5 is not null
                and mkt_share is not null
                and REGEXP_COUNT(actual_value, '^[0.00-9]+$') > 0
                and ques_type in ('DENOMINATOR', 'NUMERATOR')
                --        and country = 'Taiwan'
                and country in ('Hong Kong', 'Korea', 'Taiwan')
            group by
                country, customername, scheduleddate, prod_hier_l4, prod_hier_l5
            having COUNT(distinct ques_type) = 2
        )
)

select * from final
