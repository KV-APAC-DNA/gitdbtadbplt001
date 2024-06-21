with wks_edw_perfect_store_hash as (
    select * from snapaspwks_integration.wks_edw_perfect_store_hash
),
cte1 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (country, customerid, scheduleddate, PROD_HIER_L4) ---- franchise to be replaced by category
    in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L4
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country = 'Indonesia'
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L4
        having count (distinct QUES_TYPE) = 2
    )
),
cte2 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (
        country,
        customerid,
        scheduleddate,
        PROD_HIER_L3,
        PROD_HIER_L4
    ) in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L3,
            PROD_HIER_L4
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country = 'Malaysia'
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L3,
            PROD_HIER_L4
        having count (distinct QUES_TYPE) = 2
    )
),
cte3 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (country, customerid, scheduleddate, PROD_HIER_L3) ---- franchise to be replaced by category
    in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L3
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country = 'India'
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L3
        having count (distinct QUES_TYPE) = 2
    )
),
cte4 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (
        country,
        customerid,
        scheduleddate,
        PROD_HIER_L3,
        PROD_HIER_L4
    ) -- l3,l4 to be replaced by cat, seg
    in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L3,
            PROD_HIER_L4
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country in ('Australia', 'New Zealand')
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L3,
            PROD_HIER_L4
        having count (distinct QUES_TYPE) = 2
    )
),
cte5 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (
        country,
        customerid,
        scheduleddate,
        PROD_HIER_L4,
        nvl(PROD_HIER_L5, 'x')
    ) -- l3,l4 to be replaced by cat, seg
    in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L4,
            nvl(PROD_HIER_L5, 'x')
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country in ('Japan', 'Singapore', 'Thailand', 'Vietnam')
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L4,
            PROD_HIER_L5
        having count (distinct QUES_TYPE) = 2
    )
),
cte6 as (
    select * --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
from rg_wks.wks_edw_perfect_store_hash
where kpi in ('SOS COMPLIANCE')
    and mkt_share is NOT NULL
    and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
    and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
    and (country, customerid, scheduleddate, PROD_HIER_L5) -- l3,l4 to be replaced by cat, seg
    in (
        select country,
            customerid,
            scheduleddate,
            PROD_HIER_L5
        from rg_wks.wks_edw_perfect_store_hash
        where kpi in ('SOS COMPLIANCE')
            and mkt_share is NOT NULL
            and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
            and QUES_TYPE in ('DENOMINATOR', 'NUMERATOR')
            and country = 'China'
        group by country,
            customerid,
            scheduleddate,
            PROD_HIER_L5
        having count (distinct QUES_TYPE) = 2
    )
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
)
select * from final