-- Import CTE
with
    wks_edw_perfect_store_hash as (
        select *
        from {{ ref("stg_arsadpprd001_raw__rg_wks_wks_edw_perfect_store_hash") }}
    ),
    wks_edw_perfect_store_kpi_rebased_wt_mnth_cname as (
        select * from {{ ref("wks_edw_perfect_store_kpi_rebased_wt_mnth_cname") }}
    ),
    wks_perfect_store_sos_soa_mnth as (
        select * from {{ ref("wks_perfect_store_sos_soa_mnth") }}
    ),
    -- Logical CTE 
    msl_oos as (
        select
            per_str.*,
            reb_wt.total_weight,
            calc_weight,
            weight_msl,
            weight_oos,
            weight_soa,
            weight_sos,
            weight_promo,
            weight_planogram,
            weight_display
        from
            (
                select *
                from wks_edw_perfect_store_hash
                where
                    kpi in ('MSL COMPLIANCE', 'OOS COMPLIANCE')
                    -- and nvl(kpi_chnl_wt,0) > 0 
                    and country in ('Hong Kong', 'Korea', 'Taiwan')
            ) per_str,
            wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
        where
            per_str.country = reb_wt.country
            and per_str.customername = reb_wt.customername
            and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
            and per_str.kpi = reb_wt.kpi
    ),

    -- Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
    promo_plano as (
        select
            per_str.*,
            reb_wt.total_weight,
            calc_weight,
            weight_msl,
            weight_oos,
            weight_soa,
            weight_sos,
            weight_promo,
            weight_planogram,
            weight_display
        from
            (
                select *
                from wks_edw_perfect_store_hash
                where
                    kpi in (
                        'PROMO COMPLIANCE', 'PLANOGRAM COMPLIANCE', 'DISPLAY COMPLIANCE'
                    )
                    and ref_value = 1
                    -- and nvl(kpi_chnl_wt,0) > 0  
                    and country in ('Hong Kong', 'Korea', 'Taiwan')
            ) per_str,
            wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
        where
            per_str.country = reb_wt.country
            and per_str.customername = reb_wt.customername
            and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
            and per_str.kpi = reb_wt.kpi

    ),

    -- Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
    display_rem as (
        select per_st.*
        from wks_edw_perfect_store_hash per_st
        where
            kpi in ('PROMO COMPLIANCE', 'PLANOGRAM COMPLIANCE', 'DISPLAY COMPLIANCE')
            and country in ('Hong Kong', 'Korea', 'Taiwan')
        minus
        select per_str.*
        from wks_edw_perfect_store_hash per_str
        where
            kpi in ('PROMO COMPLIANCE', 'PLANOGRAM COMPLIANCE', 'DISPLAY COMPLIANCE')
            and ref_value = 1
            -- and nvl(kpi_chnl_wt,0) > 0  
            and country in ('Hong Kong', 'Korea', 'Taiwan')
    ),

    display as (
        select
            *,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from display_rem
    ),

    -- Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA
    soa as (
        select
            per_str.*,
            reb_wt.total_weight,
            calc_weight,
            weight_msl,
            weight_oos,
            weight_soa,
            weight_sos,
            weight_promo,
            weight_planogram,
            weight_display
        from
            wks_perfect_store_sos_soa_mnth per_str,
            wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
        where
            per_str.country = reb_wt.country
            and per_str.customername = reb_wt.customername
            and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
            and per_str.kpi = reb_wt.kpi

    ),

    -- Insert into rg_edw.EDW_PERFECT_STORE_KPI_DATA 
    sos_soa_rem as (
        select *  -- country, customerid, scheduleddate, kpi, kpi_chnl_wt 
        from wks_edw_perfect_store_hash
        where
            kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
            and country in ('Hong Kong', 'Korea', 'Taiwan')
        minus
        select *
        from wks_perfect_store_sos_soa_mnth
    ),
    sos_soa as (
        select
            *,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from sos_soa_rem
    ),

    -- Final CTE
    final as (
        select *
        from msl_oos
        union all
        select *
        from promo_plano
        union all
        select *
        from display
        union all
        select *
        from soa
        union all
        select *
        from sos_soa
    )

select *
from final
