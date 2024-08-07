with wks_edw_perfect_store_kpi_wt_mnth as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_kpi_wt_mnth') }}
),
wks_edw_perfect_store_kpi_agg_wt_mnth as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_kpi_agg_wt_mnth') }}
),
final as (
    select wt.country,
        wt.customerid,
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
    from wks_edw_perfect_store_kpi_wt_mnth wt,
        wks_edw_perfect_store_kpi_agg_wt_mnth agg_wt
    where wt.country = agg_wt.country
        and wt.customerid = agg_wt.customerid
        and wt.scheduledmonth = agg_wt.scheduledmonth
)
select * from final