--Import CTE
with v_edw_ph_rpt_retail_excellence as (
    select *
    from {{ source('phledw_integration', 'v_edw_ph_rpt_retail_excellence') }}
),

--Logical CTE
final as (
    select
        fisc_yr,
        "CLUSTER",		
        market,
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
        NULL as prod_hier_l5,
        NULL as prod_hier_l6,
        NULL as prod_hier_l7,
        NULL as prod_hier_l8,
        NULL as prod_hier_l9,
        global_product_franchise,
        global_product_brand,
        global_product_sub_brand,
        global_product_segment,
        global_product_subsegment,
        global_product_category,
        global_product_subcategory,
        store_code,
        product_code,
        target_complaince,
        CAST(fisc_per as numeric(18, 0)) as fisc_per,
        MD5(
            COALESCE(sell_out_channel, 'soc')
            || COALESCE(retail_environment, 're')
            || COALESCE(region, 'reg')
            || COALESCE(zone_name, 'zn')
            || COALESCE(city, 'cty')
            || COALESCE(prod_hier_l1, 'ph1')
            || COALESCE(prod_hier_l2, 'ph2')
            || COALESCE(prod_hier_l3, 'ph3')
            || COALESCE(prod_hier_l4, 'ph4')
            || COALESCE(global_product_franchise, 'gpf')
            || COALESCE(global_product_brand, 'gpb')
            || COALESCE(global_product_sub_brand, 'gpsb')
            || COALESCE(global_product_segment, 'gps')
            || COALESCE(global_product_subsegment, 'gpss')
            || COALESCE(global_product_category, 'gpc')
            || COALESCE(global_product_subcategory, 'gpsc')
        ) as flag_agg_dim_key,
        SUM(sales_value) as sales_value,
        SUM(sales_qty) as sales_qty,		--// AVG
        AVG(sales_qty) as avg_sales_qty,
        SUM(lm_sales) as lm_sales,
        SUM(lm_sales_qty) as lm_sales_qty,		--// AVG
        AVG(lm_sales_qty) as lm_avg_sales_qty,
        SUM(p3m_sales) as p3m_sales,
        SUM(p3m_qty) as p3m_qty,		--// AVG
        AVG(p3m_qty) as p3m_avg_qty,
        SUM(p6m_sales) as p6m_sales,
        SUM(p6m_qty) as p6m_qty,		--// AVG
        (AVG(p6m_qty)) as p6m_avg_qty,
        SUM(p12m_sales) as p12m_sales,
        SUM(p12m_qty) as p12m_qty,		--// AVG
        AVG(p12m_qty) as p12m_avg_qty,
        SUM(f3m_sales) as f3m_sales,
        SUM(f3m_qty) as f3m_qty,		--// AVG
        AVG(f3m_qty) as f3m_avg_qty,
        MAX(list_price) as list_price,
        case
            when SUM(
                case
                    when lm_sales_flag = 'Y' then 1
                    else 0
                end
            ) > 0 then 1
            else 0
        end as lm_sales_flag,
        case
            when SUM(
                case
                    when p3m_sales_flag = 'Y' then 1
                    else 0
                end
            ) > 0 then 1
            else 0
        end as p3m_sales_flag,
        case
            when SUM(
                case
                    when p6m_sales_flag = 'Y' then 1
                    else 0
                end
            ) > 0 then 1
            else 0
        end as p6m_sales_flag,
        case
            when SUM(
                case
                    when p12m_sales_flag = 'Y' then 1
                    else 0
                end
            ) > 0 then 1
            else 0
        end as p12m_sales_flag,
        case
            when mdp_flag = 'Y' then 1
            else 0
        end as mdp_flag,
        MAX(size_of_price_lm) as size_of_price_lm,
        MAX(size_of_price_p3m) as size_of_price_p3m,
        MAX(size_of_price_p6m) as size_of_price_p6m,
        MAX(size_of_price_p12m) as size_of_price_p12m,
        SUM(sales_value_list_price) as sales_value_list_price,
        SUM(lm_sales_lp) as lm_sales_lp,
        SUM(p3m_sales_lp) as p3m_sales_lp,
        SUM(p6m_sales_lp) as p6m_sales_lp,
        SUM(p12m_sales_lp) as p12m_sales_lp,
        MAX(size_of_price_lm_lp) as size_of_price_lm_lp,
        MAX(size_of_price_p3m_lp) as size_of_price_p3m_lp,
        MAX(size_of_price_p6m_lp) as size_of_price_p6m_lp,
        MAX(size_of_price_p12m_lp) as size_of_price_p12m_lp,
        SYSDATE()		--// SYSDATE
    --// FROM OS_EDW.EDW_PH_RPT_RETAIL_EXCELLENCE FLAGS
    from v_edw_ph_rpt_retail_excellence as flags

    group by
        flags.fisc_yr,		--// GROUP BY FLAGS.FISC_YR,
        flags.fisc_per,		--//          FLAGS.FISC_PER,
        flags."CLUSTER",		--//          FLAGS."CLUSTER",
        flags.market,		--//          FLAGS.MARKET,
        MD5(
            COALESCE(sell_out_channel, 'soc')
            || COALESCE(retail_environment, 're')
            || COALESCE(region, 'reg')
            || COALESCE(zone_name, 'zn')
            || COALESCE(city, 'cty')
            || COALESCE(prod_hier_l1, 'ph1')
            || COALESCE(prod_hier_l2, 'ph2')
            || COALESCE(prod_hier_l3, 'ph3')
            || COALESCE(prod_hier_l4, 'ph4')
            || COALESCE(global_product_franchise, 'gpf')
            || COALESCE(global_product_brand, 'gpb')
            || COALESCE(global_product_sub_brand, 'gpsb')
            || COALESCE(global_product_segment, 'gps')
            || COALESCE(global_product_subsegment, 'gpss')
            || COALESCE(global_product_category, 'gpc')
            || COALESCE(global_product_subcategory, 'gpsc')
        ),
        flags.data_src,		--//          FLAGS.DATA_SRC,
        flags.distributor_code,		--//          FLAGS.DISTRIBUTOR_CODE,
        flags.distributor_name,		--//          FLAGS.DISTRIBUTOR_NAME,
        flags.sell_out_channel,		--//          FLAGS.SELL_OUT_CHANNEL,
        flags.region,		--//          FLAGS.REGION,
        flags.zone_name,		--//          FLAGS.ZONE_NAME,
        flags.city,		--//          FLAGS.CITY,
        flags.retail_environment,		--//          FLAGS.RETAIL_ENVIRONMENT,
        flags.prod_hier_l1,		--//          FLAGS.PROD_HIER_L1,
        flags.prod_hier_l2,		--//          FLAGS.PROD_HIER_L2,
        flags.prod_hier_l3,		--//          FLAGS.PROD_HIER_L3,
        flags.prod_hier_l4,		--//          FLAGS.PROD_HIER_L4,
        --//          FLAGS.GLOBAL_PRODUCT_FRANCHISE,
        flags.global_product_franchise,
        flags.global_product_brand,		--//          FLAGS.GLOBAL_PRODUCT_BRAND,
        --//          FLAGS.GLOBAL_PRODUCT_SUB_BRAND,
        flags.global_product_sub_brand,
        --//          FLAGS.GLOBAL_PRODUCT_SEGMENT,
        flags.global_product_segment,
        --//          FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,
        flags.global_product_subsegment,
        --//          FLAGS.GLOBAL_PRODUCT_CATEGORY,
        flags.global_product_category,
        --//          FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,
        flags.global_product_subcategory,
        (case when mdp_flag = 'Y' then 1 else 0 end),
        target_complaince,
        store_code,
        product_code
)


--Final select
select * from final
