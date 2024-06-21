with wks_edw_perfect_store_hash as (
    select * from snapaspwks_integration.wks_edw_perfect_store_hash
),
cte1 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where --        nvl(kpi_chnl_wt,0) > 0 
        --   and  
        kpi in ('MSL COMPLIANCE', 'OOS COMPLIANCE') --and country = 'Korea' 
        and country not in ('Hong Kong', 'Korea', 'Taiwan')
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte2 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where --        nvl(kpi_chnl_wt,0) > 0  
        --   and  
        kpi in ('PROMO COMPLIANCE', 'DISPLAY COMPLIANCE')
        and REF_VALUE = 1 --  and country = 'Taiwan'  
        and country not in ('Hong Kong', 'Korea', 'Taiwan')
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte3 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where --        nvl(kpi_chnl_wt,0) > 0  
        --   and  
        kpi in ('PLANOGRAM COMPLIANCE')
        and REF_VALUE = 1 --  and country = 'Taiwan'  
        and country not in (
            'Hong Kong',
            'Korea',
            'Taiwan',
            'Australia',
            'New Zealand',
            'China'
        )
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte4 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where kpi = 'PLANOGRAM COMPLIANCE'
        and mkt_share is not null
        and country in ('Australia', 'New Zealand')
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte5 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where kpi = 'PLANOGRAM COMPLIANCE'
        and country = 'China'
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte6 as (
    select country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    from wks_edw_perfect_store_hash
    where --       nvl(kpi_chnl_wt,0) > 0 
        --       and  
        kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
        and mkt_share is NOT NULL
        and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
        and country = 'Philippines'
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
cte7 as (
    SELECT country,
        nvl(customerid, 'x') as customerid,
        to_char(scheduleddate, 'YYYYMM') scheduledmonth,
        kpi,
        min(cast(kpi_chnl_wt as numeric(8, 4))) chnl_wt
    FROM wks_perfect_store_sos_soa_custid_ind
    group by country,
        customerid,
        to_char(scheduleddate, 'YYYYMM'),
        kpi
),
final as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
    union all
    select * from cte5
    union all
    select * from cte6
    union all
    select * from cte7
)
select * from final