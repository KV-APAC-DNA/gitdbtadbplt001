with wks_edw_perfect_store_hash as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_hash') }}
),
wks_perfect_store_sos_soa_mnth as (
    select * from {{ ref('aspwks_integration__wks_perfect_store_sos_soa_mnth') }}
),
cte1 as (
    select country,
        nvl(customername, 'x') as customername,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(trunc(kpi_chnl_wt, 4)) chnl_wt
    from wks_edw_perfect_store_hash
    where --        nvl(kpi_chnl_wt,0) > 0 
        --   and  
        kpi in ('MSL COMPLIANCE', 'OOS COMPLIANCE')
        and country in ('Hong Kong', 'Korea', 'Taiwan')
    group by country,
        customername,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte2 as (
    select country,
        nvl(customername, 'x') as customername,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(trunc(kpi_chnl_wt, 4)) chnl_wt
    from wks_edw_perfect_store_hash
    where --        nvl(kpi_chnl_wt,0) > 0  
        --   and  
        kpi in (
            'PROMO COMPLIANCE',
            'PLANOGRAM COMPLIANCE',
            'DISPLAY COMPLIANCE'
        )
        and REF_VALUE = 1
        and country in ('Hong Kong', 'Korea', 'Taiwan')
    group by country,
        customername,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte3 as (
    SELECT country,
        nvl(customername, 'x') as customername,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(trunc(kpi_chnl_wt, 4)) chnl_wt
    FROM wks_perfect_store_sos_soa_mnth
    group by country,
        customername,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
final as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
)
select * from final