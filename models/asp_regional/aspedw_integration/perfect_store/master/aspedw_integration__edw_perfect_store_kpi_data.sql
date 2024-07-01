with wks_edw_perfect_store_hash as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_hash') }}
),
wks_edw_perfect_store_kpi_rebased_wt_mnth_cname as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_kpi_rebased_wt_mnth_cname') }}
),
wks_perfect_store_sos_soa_mnth as (
    select * from {{ ref('aspwks_integration__wks_perfect_store_sos_soa_mnth') }}
),
wks_perfect_store_sos_soa_custid_ind as (
    select * from {{ ref('aspwks_integration__wks_perfect_store_sos_soa_custid_ind') }}
),
wks_edw_perfect_store_kpi_rebased_wt_mnth as (
    select * from {{ ref('aspwks_integration__wks_edw_perfect_store_kpi_rebased_wt_mnth') }}
),
cte1 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi in ('MSL COMPLIANCE', 'OOS COMPLIANCE') --and nvl(kpi_chnl_wt,0) > 0 
                and country in ('Hong Kong', 'Korea', 'Taiwan')
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
    where per_str.country = reb_wt.country
        and per_str.customername = reb_wt.customername
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte2 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi in (
                    'PROMO COMPLIANCE',
                    'PLANOGRAM COMPLIANCE',
                    'DISPLAY COMPLIANCE'
                )
                and REF_VALUE = 1 --and nvl(kpi_chnl_wt,0) > 0  
                and country in ('Hong Kong', 'Korea', 'Taiwan')
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
    where per_str.country = reb_wt.country
        and per_str.customername = reb_wt.customername
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte3 as (
    select per_st.*,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_edw_perfect_store_hash per_st
    where kpi in (
            'PROMO COMPLIANCE',
            'PLANOGRAM COMPLIANCE',
            'DISPLAY COMPLIANCE'
        )
        and country in ('Hong Kong', 'Korea', 'Taiwan')
    minus
    select per_str.*,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_edw_perfect_store_hash per_str
    where kpi in (
            'PROMO COMPLIANCE',
            'PLANOGRAM COMPLIANCE',
            'DISPLAY COMPLIANCE'
        )
        and REF_VALUE = 1 --and nvl(kpi_chnl_wt,0) > 0  
        and country in ('Hong Kong', 'Korea', 'Taiwan')
),
cte4 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from wks_perfect_store_sos_soa_mnth per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth_cname reb_wt
    where per_str.country = reb_wt.country
        and per_str.customername = reb_wt.customername
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte5 as (
    select *, --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_edw_perfect_store_hash
    where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
        and country in ('Hong Kong', 'Korea', 'Taiwan')
    minus
    select *,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_perfect_store_sos_soa_mnth
),
cte6 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi in ('MSL COMPLIANCE', 'OOS COMPLIANCE')
                and country not in ('Hong Kong', 'Korea', 'Taiwan') --and nvl(kpi_chnl_wt,0) > 0 
                -- and country = 'Korea'
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte7 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi in ('PROMO COMPLIANCE', 'DISPLAY COMPLIANCE')
                and country not in ('Hong Kong', 'Korea', 'Taiwan')
                and REF_VALUE = 1 --and nvl(kpi_chnl_wt,0) > 0  
                --and country = 'Taiwan' 
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte8 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi in ('PLANOGRAM COMPLIANCE')
                and country not in (
                    'Hong Kong',
                    'Korea',
                    'Taiwan',
                    'Australia',
                    'New Zealand',
                    'China'
                )
                and REF_VALUE = 1 --and nvl(kpi_chnl_wt,0) > 0  
                --and country = 'Taiwan' 
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi
),
cte9 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi = 'PLANOGRAM COMPLIANCE'
                and mkt_share is not null
                and country in ('Australia', 'New Zealand') --and nvl(kpi_chnl_wt,0) > 0  
                --and country = 'Taiwan' 
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte10 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash
            where kpi = 'PLANOGRAM COMPLIANCE'
                and country = 'China' --and nvl(kpi_chnl_wt,0) > 0  
                --and country = 'Taiwan' 
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte11 as (
    select per_st.*,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_edw_perfect_store_hash per_st
    where kpi in (
            'PROMO COMPLIANCE',
            'DISPLAY COMPLIANCE',
            'PLANOGRAM COMPLIANCE'
        )
        and country not in ('Hong Kong', 'Korea', 'Taiwan') -- and country = 'Korea'
    minus
    (
        select per_str.*,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from wks_edw_perfect_store_hash per_str
        where kpi in ('PLANOGRAM COMPLIANCE')
            and country not in (
                'Hong Kong',
                'Korea',
                'Taiwan',
                'Australia',
                'New Zealand',
                'China'
            )
            and REF_VALUE = 1
        union all
        select per_str.*,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from wks_edw_perfect_store_hash per_str
        where kpi in ('PROMO COMPLIANCE', 'DISPLAY COMPLIANCE')
            and country not in ('Hong Kong', 'Korea', 'Taiwan')
            and REF_VALUE = 1 --and nvl(kpi_chnl_wt,0) > 0  
            --and country = 'Korea'
        union all
        select per_str.*,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from wks_edw_perfect_store_hash per_str
        where kpi = 'PLANOGRAM COMPLIANCE'
            and mkt_share is not null
            and country in ('Australia', 'New Zealand')
        union all
        select per_str.*,
            null as total_weight,
            null as calc_weight,
            null as weight_msl,
            null as weight_oos,
            null as weight_soa,
            null as weight_sos,
            null as weight_promo,
            null as weight_planogram,
            null as weight_display
        from wks_edw_perfect_store_hash per_str
        where kpi = 'PLANOGRAM COMPLIANCE'
            and country = 'China'
    )
),
cte12 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from (
            select *
            from wks_edw_perfect_store_hash per_st
            where per_st.kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
                and per_st.mkt_share is NOT NULL --and  nvl(kpi_chnl_wt,0) > 0 
                and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
                and per_st.country = 'Philippines'
        ) per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte13 as (
    SELECT *,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    FROM wks_edw_perfect_store_hash per_str
    WHERE country = 'Philippines'
        and per_str.kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
    minus
    SELECT *,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    FROM wks_edw_perfect_store_hash per_st
    WHERE country = 'Philippines'
        and kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
        and per_st.mkt_share is NOT NULL --and  nvl(kpi_chnl_wt,0) > 0 
        and REGEXP_COUNT(ACTUAL_VALUE, '^[0.00-9]+$') > 0
),
cte14 as (
    select per_str.*,
        reb_wt.total_weight,
        calc_weight,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display
    from wks_perfect_store_sos_soa_custid_ind per_str,
        wks_edw_perfect_store_kpi_rebased_wt_mnth reb_wt
    where per_str.country = reb_wt.country
        and per_str.customerid = reb_wt.customerid
        and to_char(per_str.scheduleddate, 'YYYYMM') = reb_wt.scheduledmonth
        and per_str.kpi = reb_wt.kpi -- and per_str.customerid = 'f0dfe236-d8e9-4af9-8146-a7e00de3541d-MySales' 
        -- and per_str.scheduleddate = '2019-05-28'
),
cte15 as (
    select *, --country, customerid, scheduleddate, kpi, kpi_chnl_wt 
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_edw_perfect_store_hash
    where kpi in ('SOS COMPLIANCE', 'SOA COMPLIANCE')
        and country in(
            'Indonesia',
            'Australia',
            'New Zealand',
            'China',
            'Japan',
            'Malaysia',
            'India',
            'Singapore',
            'Thailand',
            'Vietnam'
        )
    minus
    select *,
        null as total_weight,
        null as calc_weight,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display
    from wks_perfect_store_sos_soa_custid_ind
),
transformed as (
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
    union all
    select * from cte8
    union all
    select * from cte9
    union all
    select * from cte10
    union all
    select * from cte11
    union all
    select * from cte12
    union all
    select * from cte13
    union all
    select * from cte14
    union all
    select * from cte15
),
final as (
    select
        hashkey::varchar(32) as hashkey,
        hash_row::number(19,0) as hash_row,
        dataset::varchar(128) as dataset,
        customerid::varchar(255) as customerid,
        salespersonid::varchar(255) as salespersonid,
        visitid::varchar(255) as visitid,
        questiontext::varchar(513) as questiontext,
        productid::varchar(255) as productid,
        kpi::varchar(573) as kpi,
        scheduleddate::timestamp_ntz(9) as scheduleddate,
        latestdate::timestamp_ntz(9) as latestdate,
        fisc_yr::number(10,0) as fisc_yr,
        fisc_per::number(10,0) as fisc_per,
        merchandiser_name::varchar(770) as merchandiser_name,
        customername::varchar(771) as customername,
        country::varchar(200) as country,
        state::varchar(256) as state,
        parent_customer::varchar(382) as parent_customer,
        retail_environment::varchar(382) as retail_environment,
        channel::varchar(382) as channel,
        retailer::varchar(382) as retailer,
        business_unit::varchar(200) as business_unit,
        eannumber::varchar(255) as eannumber,
        matl_num::varchar(100) as matl_num,
        prod_hier_l1::varchar(500) as prod_hier_l1,
        prod_hier_l2::varchar(500) as prod_hier_l2,
        prod_hier_l3::varchar(510) as prod_hier_l3,
        prod_hier_l4::varchar(510) as prod_hier_l4,
        prod_hier_l5::varchar(2000) as prod_hier_l5,
        prod_hier_l6::varchar(500) as prod_hier_l6,
        prod_hier_l7::varchar(500) as prod_hier_l7,
        prod_hier_l8::varchar(500) as prod_hier_l8,
        prod_hier_l9::varchar(2258) as prod_hier_l9,
        ques_type::varchar(382) as ques_type,
        "y/n_flag"::varchar(150) as "y/n_flag",
        priority_store_flag::varchar(256) as priority_store_flag,
        add_info::varchar(65535) as add_info,
        response::varchar(65535) as response,
        response_score::varchar(65535) as response_score,
        kpi_chnl_wt::float as kpi_chnl_wt,
        mkt_share::float as mkt_share,
        salience_val::number(20,4) as salience_val,
        channel_weightage::number(28,2) as channel_weightage,
        actual_value::number(14,4) as actual_value,
        ref_value::number(22,4) as ref_value,
        kpi_actual_wt_val::varchar(40) as kpi_actual_wt_val,
        kpi_ref_val::varchar(40) as kpi_ref_val,
        kpi_ref_wt_val::varchar(150) as kpi_ref_wt_val,
        photo_url::varchar(500) as photo_url,
        store_grade::varchar(50) as store_grade,
        total_weight::number(20,4) as total_weight,
        calc_weight::number(11,6) as calc_weight,
        weight_msl::number(19,10) as weight_msl,
        weight_oos::number(19,10) as weight_oos,
        weight_soa::number(19,10) as weight_soa,
        weight_sos::number(19,10) as weight_sos,
        weight_promo::number(19,10) as weight_promo,
        weight_planogram::number(19,10) as weight_planogram,
        weight_display::number(19,10) as weight_display
    from transformed
)
select * from final