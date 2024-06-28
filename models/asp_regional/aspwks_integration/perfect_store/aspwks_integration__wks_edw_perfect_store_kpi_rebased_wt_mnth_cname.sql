with wks_edw_perfect_store_kpi_agg_wt_mnth_cname as (
    select * from snapaspwks_integration.wks_edw_perfect_store_kpi_agg_wt_mnth_cname
),
wks_edw_perfect_store_kpi_wt_mnth_cname as (
    select * from snapaspwks_integration.wks_edw_perfect_store_kpi_wt_mnth_cname
),
final as (
    select wt.country,
        wt.customername,
        wt.scheduledmonth,
        wt.kpi,
        wt.chnl_wt,
        agg_wt.total_weight,
        agg_wt.calc_weight,
        case
            when kpi = 'MSL COMPLIANCE' then chnl_wt * calc_weight
        end as weight_msl,
        case
            when kpi = 'OOS COMPLIANCE' then chnl_wt * calc_weight
        end as weight_oos,
        case
            when kpi = 'SOA COMPLIANCE' then chnl_wt * calc_weight
        end as weight_soa,
        case
            when kpi = 'SOS COMPLIANCE' then chnl_wt * calc_weight
        end as weight_sos,
        case
            when kpi = 'PROMO COMPLIANCE' then chnl_wt * calc_weight
        end as weight_promo,
        case
            when kpi = 'PLANOGRAM COMPLIANCE' then chnl_wt * calc_weight
        end as weight_planogram,
        case
            when kpi = 'DISPLAY COMPLIANCE' then chnl_wt * calc_weight
        end as weight_display
    from wks_edw_perfect_store_kpi_wt_mnth_cname wt,
        wks_edw_perfect_store_kpi_agg_wt_mnth_cname agg_wt
    where wt.country = agg_wt.country
        and wt.customername = agg_wt.customername
        and wt.scheduledmonth = agg_wt.scheduledmonth
)
select * from final