with wks_edw_perfect_store_kpi_wt_mnth as (
    select * from snapaspwks_integration.wks_edw_perfect_store_kpi_wt_mnth
),
final as (
    select country,
        customerid,
        scheduledmonth,
        sum(chnl_wt) total_weight,
        case
            when sum(chnl_wt) = 1 then 1
            when sum(chnl_wt) <= 0 then 0
            else 1 / sum(chnl_wt)
        end as calc_weight
    from wks_edw_perfect_store_kpi_wt_mnth
    group by country,
        customerid,
        scheduledmonth
)
select * from final