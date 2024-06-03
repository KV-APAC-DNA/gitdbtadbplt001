--Import CTE
with ph_edw_rpt_retail_excellence_summary_base as (
    select *
    from
        {{ ref('aspedw_integration__edw_ph_rpt_retail_excellence_summary_base') }}
),

--Final CTE
ph_edw_rpt_retail_excellence_summary as (
    select
        fisc_yr,
        fisc_per,
        "CLUSTER",
        market,
        data_src,
        flag_agg_dim_key,
        distributor_code,
        distributor_name,
        sell_out_channel,
        region,
        zone_name,
        city,
        retail_environment,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        global_product_franchise,
        global_product_brand,
        global_product_sub_brand,
        global_product_segment,
        global_product_subsegment,
        global_product_category,
        global_product_subcategory,
        target_complaince,
        case when lm_sales_flag = 1 then 'Y' else 'N' end as lm_sales_flag,
        case when p3m_sales_flag = 1 then 'Y' else 'N' end as p3m_sales_flag,
        case when p6m_sales_flag = 1 then 'Y' else 'N' end as p6m_sales_flag,
        case when p12m_sales_flag = 1 then 'Y' else 'N' end as p12m_sales_flag,
        case when mdp_flag = 1 then 'Y' else 'N' end as mdp_flag,
        SUM(sales_value) as sales_value,
        SUM(sales_qty) as sales_qty,
        AVG(sales_qty) as avg_sales_qty,		--// AVG
        SUM(lm_sales) as lm_sales,
        SUM(lm_sales_qty) as lm_sales_qty,
        AVG(lm_sales_qty) as lm_avg_sales_qty,		--// AVG
        SUM(p3m_sales) as p3m_sales,
        SUM(p3m_qty) as p3m_qty,
        AVG(p3m_qty) as p3m_avg_qty,		--// AVG
        SUM(p6m_sales) as p6m_sales,
        SUM(p6m_qty) as p6m_qty,
        AVG(p6m_qty) as p6m_avg_qty,		--// AVG
        SUM(p12m_sales) as p12m_sales,
        SUM(p12m_qty) as p12m_qty,
        AVG(p12m_qty) as p12m_avg_qty,		--// AVG
        SUM(f3m_sales) as f3m_sales,
        SUM(f3m_qty) as f3m_qty,
        AVG(f3m_qty) as f3m_avg_qty,		--// AVG
        SUM(size_of_price_lm) as size_of_price_lm,
        SUM(size_of_price_p3m) as size_of_price_p3m,
        SUM(size_of_price_p6m) as size_of_price_p6m,
        SUM(size_of_price_p12m) as size_of_price_p12m,
        COUNT(lm_sales_flag) as lm_sales_flag_count,
        COUNT(p3m_sales_flag) as p3m_sales_flag_count,
        COUNT(p6m_sales_flag) as p6m_sales_flag_count,
        COUNT(p12m_sales_flag) as p12m_sales_flag_count,
        COUNT(mdp_flag) as mdp_flag_count,
        MAX(list_price) as list_price,
        SUM(sales_value_list_price) as sales_value_list_price,
        SUM(lm_sales_lp) as lm_sales_lp,
        SUM(p3m_sales_lp) as p3m_sales_lp,
        SUM(p6m_sales_lp) as p6m_sales_lp,
        SUM(p12m_sales_lp) as p12m_sales_lp,
        SUM(size_of_price_lm_lp) as size_of_price_lm_lp,
        SUM(size_of_price_p3m_lp) as size_of_price_p3m_lp,
        SUM(size_of_price_p6m_lp) as size_of_price_p6m_lp,
        SUM(size_of_price_p12m_lp) as size_of_price_p12m_lp,
        SYSDATE()		--// SYSDATE
    --// FROM OS_EDW.EDW_PH_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE
    from ph_edw_rpt_retail_excellence_summary_base
    where
        fisc_per
        > TO_CHAR(ADD_MONTHS((
            select TO_DATE(TO_CHAR(MAX(fisc_per)), 'YYYYMM')
            from ph_edw_rpt_retail_excellence_summary_base
        ), -15), 'YYYYMM')		--//                                      FROM OS_EDW.EDW_PH_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE),-15),'YYYYMM')
        and fisc_per <= (
            select MAX(fisc_per)
            from ph_edw_rpt_retail_excellence_summary_base
        )		--//                    FROM OS_EDW.EDW_PH_RPT_RETAIL_EXCELLENCE_SUMMARY_BASE)
    group by
        fisc_yr,
        fisc_per,
        "CLUSTER",
        market,
        flag_agg_dim_key,
        data_src,
        distributor_code,
        distributor_name,
        sell_out_channel,
        region,
        zone_name,
        city,
        retail_environment,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        global_product_franchise,
        global_product_brand,
        global_product_sub_brand,
        global_product_segment,
        global_product_subsegment,
        global_product_category,
        global_product_subcategory,
        lm_sales_flag,
        p3m_sales_flag,
        p6m_sales_flag,
        p12m_sales_flag,
        mdp_flag,
        target_complaince

)


--Final select
select * from ph_edw_rpt_retail_excellence_summary
