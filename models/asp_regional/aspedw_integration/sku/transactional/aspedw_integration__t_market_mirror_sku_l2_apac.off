with edw_market_mirror_fact as
(
    select * from {{ ref('aspedw_integration__edw_market_mirror_fact') }}
),
itg_lookup_category_mapping as
(
    select * from {{ ref('aspitg_integration__itg_lookup_category_mapping') }}
),
itg_mds_ap_sales_ops_map as
(
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
final as
(
    SELECT a.utag,
        a.market,
        a.channel,
        a.channel_type,
        a.channel_source,
        a.channel_description,
        a.category,
        a.period,
        a.time_period,
        a.date_type,
        a.last_period,
        a.supplier,
        a.product,
        a.manufacturer,
        a.brand,
        a.sub_brand,
        a.segment,
        a.packsize,
        a.npd,
        a.ggh_country,
        a.ggh_region,
        a.ggh_cluster,
        a.sku_value_sales_usd,
        a.sku_value_sales_lc,
        a.sku_promoted_value_sales_usd,
        a.sku_promoted_value_sales_lc,
        a.sku_base_value_sales_usd,
        a.sku_base_value_sales_lc,
        a.sku_unit_sales,
        a.sku_promoted_unit_sales,
        a.sku_base_unit_sales,
        a.sku_volume_sales,
        a.sku_promoted_volume_sales,
        a.sku_base_volume_sales,
        a.sku_numeric_distribution,
        a.sku_weighted_distribution,
        a.sku_cwd_feature_display_pct,
        a.sku_cwd_display_pct,
        a.sku_cwd_feature_pct,
        a.brd_weighted_distribution,
        a.cat_weighted_distribution,
        A.msl,
        (SELECT MAX(time_period)
            FROM edw_market_mirror_fact
            WHERE date_type = 'Monthly') AS max_time_period,
        CASE
            WHEN period IN ('M25','M26','M27','M28','M29','M30','M31','M32','M33','M34','M35','M36') THEN TRUE
            ELSE FALSE
        END AS period_mat,
        CASE
            WHEN period IN ('M31','M32','M33','M34','M35','M36') THEN TRUE
            ELSE FALSE
        END AS period_l6m,
        CASE
            WHEN period IN ('M34','M35','M36') THEN TRUE
            ELSE FALSE
        END AS period_l3m,
        CASE
            WHEN period IN ('M36') THEN TRUE
            ELSE FALSE
        END AS period_lm,
        CASE
            WHEN DATE_PART(year,a.time_period) = c.latest_year THEN TRUE
            ELSE FALSE
        END AS period_ytd,
        CASE
            WHEN period IN ('M13','M14','M15','M16','M17','M18','M19','M20','M21','M22','M23','M24') THEN TRUE
            ELSE FALSE
        END AS period_mat_ya,
        CASE
            WHEN period IN ('M19','M20','M21','M22','M23','M24') THEN TRUE
            ELSE FALSE
        END AS period_l6m_ya,
        CASE
            WHEN period IN ('M22','M23','M24') THEN TRUE
            ELSE FALSE
        END AS period_l3m_ya,
        CASE
            WHEN period IN ('M24') THEN TRUE
            ELSE FALSE
        END AS period_lm_ya,
        CASE
            WHEN DATE_PART(year,a.time_period) = c.latest_year - 1 AND DATE_PART(month,a.time_period) <= c.latest_month THEN TRUE
            ELSE FALSE
        END AS period_ytd_ya,
        CASE
            WHEN period IN ('M1','M2','M3','M4','M5','M6','M7','M8','M9','M10','M11','M12') THEN TRUE
            ELSE FALSE
        END AS period_mat_2ya,
        CASE
            WHEN period IN ('M7','M8','M9','M10','M11','M12') THEN TRUE
            ELSE FALSE
        END AS period_l6m_2ya,
        CASE
            WHEN period IN ('M10','M11','M12') THEN TRUE
            ELSE FALSE
        END AS period_l3m_2ya,
        CASE
            WHEN period IN ('M12') THEN TRUE
            ELSE FALSE
        END AS period_lm_2ya,
        CASE
            WHEN DATE_PART(year,a.time_period) = c.latest_year - 2 AND DATE_PART(month,a.time_period) <= c.latest_month THEN TRUE
            ELSE FALSE
        END AS period_ytd_2ya,
        CASE
            WHEN lower(TRIM(a.manufacturer)) = 'johnson & johnson' THEN TRUE
            ELSE FALSE
        END AS JJ_vs_Competitors,
        cat_map.stronghold AS gfo,
        a.gsr_description,
        a.gsr_level,
        a.gsr_flag,
        launch_date,
        sku_cwd_any_promo_pct,
        cat_cwd_any_promo_pct,
        seg_cwd_any_promo_pct,
        mnf_cwd_any_promo_pct,
        mnf_seg_cwd_any_promo_pct,
        brd_cwd_any_promo_pct,
        brd_seg_cwd_any_promo_pct,
        NULL AS attribute_import,
        market_map.destination_cluster AS cluster,
        a.filename,
        NULL AS channel_origin
        FROM edw_market_mirror_fact a
    LEFT JOIN itg_lookup_category_mapping cat_map
            ON UPPER (CASE WHEN cat_map.market = 'Korea' THEN 'South Korea' ELSE cat_map.market END) = 
            UPPER (CASE WHEN a.market = 'China' AND a.supplier = 'IQVIA' THEN 'China OTC'
            WHEN a.market = 'China' AND a.supplier <> 'IQVIA' THEN 'China Skincare'
            WHEN a.market = 'Japan' AND (UPPER (a.brand) like 'DR%CI%LABO' OR UPPER (a.category) like '%FACIAL%') THEN 'Japan DCL'
            ELSE a.market
            END)
            AND UPPER (cat_map.input_category) = UPPER (a.category)
            AND UPPER (CASE WHEN cat_map.input_sub_category IS NULL OR cat_map.input_sub_category = '' THEN 'NA' ELSE cat_map.input_sub_category END) = 
            UPPER (CASE WHEN a.segment IS NULL OR a.segment = '' THEN 'NA' ELSE a.segment END)
    LEFT JOIN (SELECT market,
                        category,
                        channel_description,
                        DATE_PART(YEAR,MAX(time_period)) AS latest_year,
                        DATE_PART(MONTH,MAX(time_period)) AS latest_month
                FROM edw_market_mirror_fact
                WHERE date_type = 'Monthly'
                AND   ggh_region = 'APAC'
                GROUP BY market,
                        category,
                        channel_description) c
            ON a.market = c.market
            AND a.category = c.category
            AND a.channel_description = c.channel_description 
    LEFT JOIN itg_mds_ap_sales_ops_map market_map
            ON UPPER (market_map.source_market) = UPPER (a.ggh_country)
            AND market_map.dataset = 'Market Share QSD'
    WHERE a.ggh_region = 'APAC'
    AND   channel_type != 'Redundant'
)
select utag::varchar(600) as utag,
    market::varchar(100) as market,
    channel::varchar(100) as channel,
    channel_type::varchar(100) as channel_type,
    channel_source::varchar(100) as channel_source,
    channel_description::varchar(1000) as channel_description,
    category::varchar(100) as category,
    period::varchar(50) as period,
    time_period::date as time_period,
    date_type::varchar(100) as date_type,
    last_period::date as last_period,
    supplier::varchar(100) as supplier,
    product::varchar(3000) as product,
    manufacturer::varchar(300) as manufacturer,
    brand::varchar(300) as brand,
    sub_brand::varchar(300) as sub_brand,
    segment::varchar(300) as segment,
    packsize::float as packsize,
    npd::varchar(10) as npd,
    ggh_country::varchar(250) as ggh_country,
    ggh_region::varchar(250) as ggh_region,
    ggh_cluster::varchar(250) as ggh_cluster,
    sku_value_sales_usd::float as sku_value_sales_usd,
    sku_value_sales_lc::float as sku_value_sales_lc,
    sku_promoted_value_sales_usd::float as sku_promoted_value_sales_usd,
    sku_promoted_value_sales_lc::float as sku_promoted_value_sales_lc,
    sku_base_value_sales_usd::float as sku_base_value_sales_usd,
    sku_base_value_sales_lc::float as sku_base_value_sales_lc,
    sku_unit_sales::float as sku_unit_sales,
    sku_promoted_unit_sales::float as sku_promoted_unit_sales,
    sku_base_unit_sales::float as sku_base_unit_sales,
    sku_volume_sales::float as sku_volume_sales,
    sku_promoted_volume_sales::float as sku_promoted_volume_sales,
    sku_base_volume_sales::float as sku_base_volume_sales,
    sku_numeric_distribution::float as sku_numeric_distribution,
    sku_weighted_distribution::float as sku_weighted_distribution,
    sku_cwd_feature_display_pct::float as sku_cwd_feature_display_pct,
    sku_cwd_display_pct::float as sku_cwd_display_pct,
    sku_cwd_feature_pct::float as sku_cwd_feature_pct,
    brd_weighted_distribution::float as brd_weighted_distribution,
    cat_weighted_distribution::float as cat_weighted_distribution,
    msl::varchar(10) as msl,
    max_time_period::date as max_time_period,
    period_mat::boolean as period_mat,
    period_l6m::boolean as period_l6m,
    period_l3m::boolean as period_l3m,
    period_lm::boolean as period_lm,
    period_ytd::boolean as period_ytd,
    period_mat_ya::boolean as period_mat_ya,
    period_l6m_ya::boolean as period_l6m_ya,
    period_l3m_ya::boolean as period_l3m_ya,
    period_lm_ya::boolean as period_lm_ya,
    period_ytd_ya::boolean as period_ytd_ya,
    period_mat_2ya::boolean as period_mat_2ya,
    period_l6m_2ya::boolean as period_l6m_2ya,
    period_l3m_2ya::boolean as period_l3m_2ya,
    period_lm_2ya::boolean as period_lm_2ya,
    period_ytd_2ya::boolean as period_ytd_2ya,
    jj_vs_competitors::boolean as jj_vs_competitors,
    gfo::varchar(510) as gfo,
    gsr_description::varchar(300) as gsr_description,
    gsr_level::varchar(100) as gsr_level,
    gsr_flag::boolean as gsr_flag,
    launch_date::date as launch_date,
    sku_cwd_any_promo_pct::float as sku_cwd_any_promo_pct,
    cat_cwd_any_promo_pct::float as cat_cwd_any_promo_pct,
    seg_cwd_any_promo_pct::float as seg_cwd_any_promo_pct,
    mnf_cwd_any_promo_pct::float as mnf_cwd_any_promo_pct,
    mnf_seg_cwd_any_promo_pct::float as mnf_seg_cwd_any_promo_pct,
    brd_cwd_any_promo_pct::float as brd_cwd_any_promo_pct,
    brd_seg_cwd_any_promo_pct::float as brd_seg_cwd_any_promo_pct,
    attribute_import::varchar(1) as attribute_import,
    cluster::varchar(200) as cluster,
    filename::varchar(200) as filename,
    channel_origin::varchar(1) as channel_origin
 from final