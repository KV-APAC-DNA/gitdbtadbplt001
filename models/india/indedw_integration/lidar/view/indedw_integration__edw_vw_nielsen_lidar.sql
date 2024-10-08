with edw_market_mirror_fact as (
    select * from {{ ref('aspedw_integration__edw_market_mirror_fact') }}
),

result as (
SELECT edw_market_mirror_fact.utag,
    edw_market_mirror_fact.tag,
    edw_market_mirror_fact.market,
    edw_market_mirror_fact.channel,
    edw_market_mirror_fact.channel_type,
    edw_market_mirror_fact.channel_source,
    edw_market_mirror_fact.channel_group,
    edw_market_mirror_fact.channel_description,
    edw_market_mirror_fact.category,
    edw_market_mirror_fact.real_date,
    edw_market_mirror_fact.period,
    edw_market_mirror_fact.time_period,
    edw_market_mirror_fact.time_period_type,
    edw_market_mirror_fact.date_type,
    edw_market_mirror_fact.last_period,
    edw_market_mirror_fact.supplier,
    edw_market_mirror_fact.source,
    edw_market_mirror_fact.product,
    edw_market_mirror_fact.segment,
    edw_market_mirror_fact.manufacturer,
    edw_market_mirror_fact.brand,
    edw_market_mirror_fact.sub_brand,
    edw_market_mirror_fact.packsize,
    edw_market_mirror_fact.npd,
    edw_market_mirror_fact.launch_year,
    edw_market_mirror_fact.base_flag,
    edw_market_mirror_fact.exclude_flag,
    edw_market_mirror_fact.cat_unit_type,
    edw_market_mirror_fact.cat_selling_unit,
    edw_market_mirror_fact.unit,
    edw_market_mirror_fact.attribute_1,
    edw_market_mirror_fact.attribute_1_desc,
    edw_market_mirror_fact.attribute_2,
    edw_market_mirror_fact.attribute_2_desc,
    edw_market_mirror_fact.attribute_3,
    edw_market_mirror_fact.attribute_3_desc,
    edw_market_mirror_fact.attribute_4,
    edw_market_mirror_fact.attribute_4_desc,
    edw_market_mirror_fact.attribute_5,
    edw_market_mirror_fact.attribute_5_desc,
    edw_market_mirror_fact.attribute_6,
    edw_market_mirror_fact.attribute_6_desc,
    edw_market_mirror_fact.attribute_7,
    edw_market_mirror_fact.attribute_7_desc,
    edw_market_mirror_fact.attribute_8,
    edw_market_mirror_fact.attribute_8_desc,
    edw_market_mirror_fact.attribute_9,
    edw_market_mirror_fact.attribute_9_desc,
    edw_market_mirror_fact.attribute_10,
    edw_market_mirror_fact.attribute_10_desc,
    edw_market_mirror_fact.attribute_11,
    edw_market_mirror_fact.attribute_11_desc,
    edw_market_mirror_fact.attribute_12,
    edw_market_mirror_fact.attribute_12_desc,
    edw_market_mirror_fact.attribute_13,
    edw_market_mirror_fact.attribute_13_desc,
    edw_market_mirror_fact.attribute_14,
    edw_market_mirror_fact.attribute_14_desc,
    edw_market_mirror_fact.attribute_15,
    edw_market_mirror_fact.attribute_15_desc,
    edw_market_mirror_fact.attribute_16,
    edw_market_mirror_fact.attribute_16_desc,
    edw_market_mirror_fact.attribute_17,
    edw_market_mirror_fact.attribute_17_desc,
    edw_market_mirror_fact.gph_gfo,
    edw_market_mirror_fact.gph_brand,
    edw_market_mirror_fact.gph_sub_brand,
    edw_market_mirror_fact.gph_variant,
    edw_market_mirror_fact.gph_need_state,
    edw_market_mirror_fact.gph_category,
    edw_market_mirror_fact.gph_sub_category,
    edw_market_mirror_fact.gph_segment,
    edw_market_mirror_fact.gph_sub_segment,
    edw_market_mirror_fact.ggh_country,
    edw_market_mirror_fact.ggh_region,
    edw_market_mirror_fact.ggh_cluster,
    edw_market_mirror_fact.ggh_sub_cluster,
    edw_market_mirror_fact.ggh_country_3_cd,
    edw_market_mirror_fact.ggh_country_2_cd,
    edw_market_mirror_fact.ggh_market_type,
    edw_market_mirror_fact.sku_value_sales_lc,
    edw_market_mirror_fact.cat_value_sales_lc,
    edw_market_mirror_fact.seg_value_sales_lc,
    edw_market_mirror_fact.mnf_value_sales_lc,
    edw_market_mirror_fact.mnf_seg_value_sales_lc,
    edw_market_mirror_fact.brd_value_sales_lc,
    edw_market_mirror_fact.brd_seg_value_sales_lc,
    edw_market_mirror_fact.sku_value_sales_usd,
    edw_market_mirror_fact.cat_value_sales_usd,
    edw_market_mirror_fact.seg_value_sales_usd,
    edw_market_mirror_fact.mnf_value_sales_usd,
    edw_market_mirror_fact.mnf_seg_value_sales_usd,
    edw_market_mirror_fact.brd_value_sales_usd,
    edw_market_mirror_fact.brd_seg_value_sales_usd,
    edw_market_mirror_fact.sku_promoted_value_sales_lc,
    edw_market_mirror_fact.cat_promoted_value_sales_lc,
    edw_market_mirror_fact.seg_promoted_value_sales_lc,
    edw_market_mirror_fact.mnf_promoted_value_sales_lc,
    edw_market_mirror_fact.mnf_seg_promoted_value_sales_lc,
    edw_market_mirror_fact.brd_promoted_value_sales_lc,
    edw_market_mirror_fact.brd_seg_promoted_value_sales_lc,
    edw_market_mirror_fact.sku_promoted_value_sales_usd,
    edw_market_mirror_fact.cat_promoted_value_sales_usd,
    edw_market_mirror_fact.seg_promoted_value_sales_usd,
    edw_market_mirror_fact.mnf_promoted_value_sales_usd,
    edw_market_mirror_fact.mnf_seg_promoted_value_sales_usd,
    edw_market_mirror_fact.brd_promoted_value_sales_usd,
    edw_market_mirror_fact.brd_seg_promoted_value_sales_usd,
    edw_market_mirror_fact.sku_base_value_sales_lc,
    edw_market_mirror_fact.cat_base_value_sales_lc,
    edw_market_mirror_fact.seg_base_value_sales_lc,
    edw_market_mirror_fact.mnf_base_value_sales_lc,
    edw_market_mirror_fact.mnf_seg_base_value_sales_lc,
    edw_market_mirror_fact.brd_base_value_sales_lc,
    edw_market_mirror_fact.brd_seg_base_value_sales_lc,
    edw_market_mirror_fact.sku_base_value_sales_usd,
    edw_market_mirror_fact.cat_base_value_sales_usd,
    edw_market_mirror_fact.seg_base_value_sales_usd,
    edw_market_mirror_fact.mnf_base_value_sales_usd,
    edw_market_mirror_fact.mnf_seg_base_value_sales_usd,
    edw_market_mirror_fact.brd_base_value_sales_usd,
    edw_market_mirror_fact.brd_seg_base_value_sales_usd,
    edw_market_mirror_fact.sku_unit_sales,
    edw_market_mirror_fact.cat_unit_sales,
    edw_market_mirror_fact.seg_unit_sales,
    edw_market_mirror_fact.mnf_unit_sales,
    edw_market_mirror_fact.mnf_seg_unit_sales,
    edw_market_mirror_fact.brd_unit_sales,
    edw_market_mirror_fact.brd_seg_unit_sales,
    edw_market_mirror_fact.sku_promoted_unit_sales,
    edw_market_mirror_fact.cat_promoted_unit_sales,
    edw_market_mirror_fact.seg_promoted_unit_sales,
    edw_market_mirror_fact.mnf_promoted_unit_sales,
    edw_market_mirror_fact.mnf_seg_promoted_unit_sales,
    edw_market_mirror_fact.brd_promoted_unit_sales,
    edw_market_mirror_fact.brd_seg_promoted_unit_sales,
    edw_market_mirror_fact.sku_base_unit_sales,
    edw_market_mirror_fact.cat_base_unit_sales,
    edw_market_mirror_fact.seg_base_unit_sales,
    edw_market_mirror_fact.mnf_base_unit_sales,
    edw_market_mirror_fact.mnf_seg_base_unit_sales,
    edw_market_mirror_fact.brd_base_unit_sales,
    edw_market_mirror_fact.brd_seg_base_unit_sales,
    edw_market_mirror_fact.sku_volume_sales,
    edw_market_mirror_fact.cat_volume_sales,
    edw_market_mirror_fact.seg_volume_sales,
    edw_market_mirror_fact.mnf_volume_sales,
    edw_market_mirror_fact.mnf_seg_volume_sales,
    edw_market_mirror_fact.brd_volume_sales,
    edw_market_mirror_fact.brd_seg_volume_sales,
    edw_market_mirror_fact.sku_promoted_volume_sales,
    edw_market_mirror_fact.cat_promoted_volume_sales,
    edw_market_mirror_fact.seg_promoted_volume_sales,
    edw_market_mirror_fact.mnf_promoted_volume_sales,
    edw_market_mirror_fact.mnf_seg_promoted_volume_sales,
    edw_market_mirror_fact.brd_promoted_volume_sales,
    edw_market_mirror_fact.brd_seg_promoted_volume_sales,
    edw_market_mirror_fact.sku_base_volume_sales,
    edw_market_mirror_fact.cat_base_volume_sales,
    edw_market_mirror_fact.seg_base_volume_sales,
    edw_market_mirror_fact.mnf_base_volume_sales,
    edw_market_mirror_fact.mnf_seg_base_volume_sales,
    edw_market_mirror_fact.brd_base_volume_sales,
    edw_market_mirror_fact.brd_seg_base_volume_sales,
    edw_market_mirror_fact.sku_numeric_distribution,
    edw_market_mirror_fact.cat_numeric_distribution,
    edw_market_mirror_fact.seg_numeric_distribution,
    edw_market_mirror_fact.mnf_numeric_distribution,
    edw_market_mirror_fact.mnf_seg_numeric_distribution,
    edw_market_mirror_fact.brd_numeric_distribution,
    edw_market_mirror_fact.brd_seg_numeric_distribution,
    edw_market_mirror_fact.sku_weighted_distribution,
    edw_market_mirror_fact.cat_weighted_distribution,
    edw_market_mirror_fact.seg_weighted_distribution,
    edw_market_mirror_fact.mnf_weighted_distribution,
    edw_market_mirror_fact.mnf_seg_weighted_distribution,
    edw_market_mirror_fact.brd_weighted_distribution,
    edw_market_mirror_fact.brd_seg_weighted_distribution,
    edw_market_mirror_fact.sku_cwd_feature_display_pct,
    edw_market_mirror_fact.cat_cwd_feature_display_pct,
    edw_market_mirror_fact.seg_cwd_feature_display_pct,
    edw_market_mirror_fact.mnf_cwd_feature_display_pct,
    edw_market_mirror_fact.mnf_seg_cwd_feature_display_pct,
    edw_market_mirror_fact.brd_cwd_feature_display_pct,
    edw_market_mirror_fact.brd_seg_cwd_feature_display_pct,
    edw_market_mirror_fact.sku_cwd_display_pct,
    edw_market_mirror_fact.cat_cwd_display_pct,
    edw_market_mirror_fact.seg_cwd_display_pct,
    edw_market_mirror_fact.mnf_cwd_display_pct,
    edw_market_mirror_fact.mnf_seg_cwd_display_pct,
    edw_market_mirror_fact.brd_cwd_display_pct,
    edw_market_mirror_fact.brd_seg_cwd_display_pct,
    edw_market_mirror_fact.sku_cwd_feature_pct,
    edw_market_mirror_fact.cat_cwd_feature_pct,
    edw_market_mirror_fact.seg_cwd_feature_pct,
    edw_market_mirror_fact.mnf_cwd_feature_pct,
    edw_market_mirror_fact.mnf_seg_cwd_feature_pct,
    edw_market_mirror_fact.brd_cwd_feature_pct,
    edw_market_mirror_fact.brd_seg_cwd_feature_pct,
    edw_market_mirror_fact.msl,
    edw_market_mirror_fact.filename,
    edw_market_mirror_fact.file_time_stamp,
    edw_market_mirror_fact.lcl_prod_hier_lvl_1,
    edw_market_mirror_fact.lcl_prod_hier_lvl_2,
    edw_market_mirror_fact.lcl_prod_hier_lvl_3,
    edw_market_mirror_fact.lcl_prod_hier_lvl_4,
    edw_market_mirror_fact.lcl_prod_hier_lvl_5,
    edw_market_mirror_fact.lcl_prod_hier_lvl_6,
    edw_market_mirror_fact.lcl_prod_hier_lvl_7,
    edw_market_mirror_fact.lcl_prod_attribute_1,
    edw_market_mirror_fact.lcl_prod_attribute_2,
    edw_market_mirror_fact.lcl_prod_attribute_3,
    edw_market_mirror_fact.lcl_prod_attribute_4,
    edw_market_mirror_fact.lcl_prod_attribute_5,
    edw_market_mirror_fact.delivery_status,
    edw_market_mirror_fact.key,
    edw_market_mirror_fact.origin,
    edw_market_mirror_fact.gsr_description,
    edw_market_mirror_fact.gsr_level,
    edw_market_mirror_fact.gsr_flag,
    edw_market_mirror_fact.launch_date,
    edw_market_mirror_fact.sku_cwd_any_promo_pct,
    edw_market_mirror_fact.cat_cwd_any_promo_pct,
    edw_market_mirror_fact.seg_cwd_any_promo_pct,
    edw_market_mirror_fact.mnf_cwd_any_promo_pct,
    edw_market_mirror_fact.mnf_seg_cwd_any_promo_pct,
    edw_market_mirror_fact.brd_cwd_any_promo_pct,
    edw_market_mirror_fact.brd_seg_cwd_any_promo_pct,
    edw_market_mirror_fact.sku_cwd_price_promo_pct,
    edw_market_mirror_fact.cat_cwd_price_promo_pct,
    edw_market_mirror_fact.seg_cwd_price_promo_pct,
    edw_market_mirror_fact.mnf_cwd_price_promo_pct,
    edw_market_mirror_fact.mnf_seg_cwd_price_promo_pct,
    edw_market_mirror_fact.brd_cwd_price_promo_pct,
    edw_market_mirror_fact.brd_seg_cwd_price_promo_pct,
    edw_market_mirror_fact.sku_cwd_extra_qty_pct,
    edw_market_mirror_fact.cat_cwd_extra_qty_pct,
    edw_market_mirror_fact.seg_cwd_extra_qty_pct,
    edw_market_mirror_fact.mnf_cwd_extra_qty_pct,
    edw_market_mirror_fact.mnf_seg_cwd_extra_qty_pct,
    edw_market_mirror_fact.brd_cwd_extra_qty_pct,
    edw_market_mirror_fact.brd_seg_cwd_extra_qty_pct,
    edw_market_mirror_fact.sku_cwd_display_other_pct,
    edw_market_mirror_fact.cat_cwd_display_other_pct,
    edw_market_mirror_fact.seg_cwd_display_other_pct,
    edw_market_mirror_fact.mnf_cwd_display_other_pct,
    edw_market_mirror_fact.mnf_seg_cwd_display_other_pct,
    edw_market_mirror_fact.brd_cwd_display_other_pct,
    edw_market_mirror_fact.brd_seg_cwd_display_other_pct,
    edw_market_mirror_fact.sku_cwd_display_entry_pct,
    edw_market_mirror_fact.cat_cwd_display_entry_pct,
    edw_market_mirror_fact.seg_cwd_display_entry_pct,
    edw_market_mirror_fact.mnf_cwd_display_entry_pct,
    edw_market_mirror_fact.mnf_seg_cwd_display_entry_pct,
    edw_market_mirror_fact.brd_cwd_display_entry_pct,
    edw_market_mirror_fact.brd_seg_cwd_display_entry_pct,
    edw_market_mirror_fact.sku_cwd_display_gondola_pct,
    edw_market_mirror_fact.cat_cwd_display_gondola_pct,
    edw_market_mirror_fact.seg_cwd_display_gondola_pct,
    edw_market_mirror_fact.mnf_cwd_display_gondola_pct,
    edw_market_mirror_fact.mnf_seg_cwd_display_gondola_pct,
    edw_market_mirror_fact.brd_cwd_display_gondola_pct,
    edw_market_mirror_fact.brd_seg_cwd_display_gondola_pct,
    edw_market_mirror_fact.sku_cwd_display_check_out_pct,
    edw_market_mirror_fact.cat_cwd_display_check_out_pct,
    edw_market_mirror_fact.seg_cwd_display_check_out_pct,
    edw_market_mirror_fact.mnf_cwd_display_check_out_pct,
    edw_market_mirror_fact.mnf_seg_cwd_display_check_out_pct,
    edw_market_mirror_fact.brd_cwd_display_check_out_pct,
    edw_market_mirror_fact.brd_seg_cwd_display_check_out_pct,
    edw_market_mirror_fact.sku_cwd_multibuy_pct,
    edw_market_mirror_fact.cat_cwd_multibuy_pct,
    edw_market_mirror_fact.seg_cwd_multibuy_pct,
    edw_market_mirror_fact.mnf_cwd_multibuy_pct,
    edw_market_mirror_fact.mnf_seg_cwd_multibuy_pct,
    edw_market_mirror_fact.brd_cwd_multibuy_pct,
    edw_market_mirror_fact.brd_seg_cwd_multibuy_pct,
    edw_market_mirror_fact.sku_cwd_loyalty_card_pct,
    edw_market_mirror_fact.cat_cwd_loyalty_card_pct,
    edw_market_mirror_fact.seg_cwd_loyalty_card_pct,
    edw_market_mirror_fact.mnf_cwd_loyalty_card_pct,
    edw_market_mirror_fact.mnf_seg_cwd_loyalty_card_pct,
    edw_market_mirror_fact.brd_cwd_loyalty_card_pct,
    edw_market_mirror_fact.brd_seg_cwd_loyalty_card_pct,
    edw_market_mirror_fact.channel_origin,
    edw_market_mirror_fact.sku_weighted_distribution_w_pct,
    edw_market_mirror_fact.cat_weighted_distribution_w_pct,
    edw_market_mirror_fact.seg_weighted_distribution_w_pct,
    edw_market_mirror_fact.mnf_weighted_distribution_w_pct,
    edw_market_mirror_fact.mnf_seg_weighted_distribution_w_pct,
    edw_market_mirror_fact.brd_weighted_distribution_w_pct,
    edw_market_mirror_fact.brd_seg_weighted_distribution_w_pct,
    edw_market_mirror_fact.sku_weighted_distribution_4w_pct,
    edw_market_mirror_fact.cat_weighted_distribution_4w_pct,
    edw_market_mirror_fact.seg_weighted_distribution_4w_pct,
    edw_market_mirror_fact.mnf_weighted_distribution_4w_pct,
    edw_market_mirror_fact.mnf_seg_weighted_distribution_4w_pct,
    edw_market_mirror_fact.brd_weighted_distribution_4w_pct,
    edw_market_mirror_fact.brd_seg_weighted_distribution_4w_pct,
    edw_market_mirror_fact.sku_weighted_distribution_12w_pct,
    edw_market_mirror_fact.cat_weighted_distribution_12w_pct,
    edw_market_mirror_fact.seg_weighted_distribution_12w_pct,
    edw_market_mirror_fact.mnf_weighted_distribution_12w_pct,
    edw_market_mirror_fact.mnf_seg_weighted_distribution_12w_pct,
    edw_market_mirror_fact.brd_weighted_distribution_12w_pct,
    edw_market_mirror_fact.brd_seg_weighted_distribution_12w_pct,
    edw_market_mirror_fact.sku_weighted_distribution_pct,
    edw_market_mirror_fact.cat_weighted_distribution_pct,
    edw_market_mirror_fact.seg_weighted_distribution_pct,
    edw_market_mirror_fact.mnf_weighted_distribution_pct,
    edw_market_mirror_fact.mnf_seg_weighted_distribution_pct,
    edw_market_mirror_fact.brd_weighted_distribution_pct,
    edw_market_mirror_fact.brd_seg_weighted_distribution_pct,
    edw_market_mirror_fact.manufacturer_local,
    edw_market_mirror_fact.manufacturer_global,
    edw_market_mirror_fact.brand_local,
    edw_market_mirror_fact.brand_global,
    edw_market_mirror_fact.need_state,
    edw_market_mirror_fact.gfo,
    edw_market_mirror_fact.cluster,
    edw_market_mirror_fact.sub_cluster,
    edw_market_mirror_fact.form
FROM edw_market_mirror_fact
WHERE (
        ((edw_market_mirror_fact.supplier)::TEXT = 'Nielsen'::TEXT)
        AND ((edw_market_mirror_fact.market)::TEXT = 'India'::TEXT)
        )
    ),

final as (
    select 
        UTAG::VARCHAR(600) as UTAG,
        TAG::VARCHAR(600) as TAG,
        MARKET::VARCHAR(100) as MARKET,
        CHANNEL::VARCHAR(100) as CHANNEL,
        CHANNEL_TYPE::VARCHAR(100) as CHANNEL_TYPE,
        CHANNEL_SOURCE::VARCHAR(100) as CHANNEL_SOURCE,
        CHANNEL_GROUP::VARCHAR(100) as CHANNEL_GROUP,
        CHANNEL_DESCRIPTION::VARCHAR(1000) as CHANNEL_DESCRIPTION,
        CATEGORY::VARCHAR(100) as CATEGORY,
        REAL_DATE::DATE as REAL_DATE,
        PERIOD::VARCHAR(50) as PERIOD,
        TIME_PERIOD::DATE as TIME_PERIOD,
        TIME_PERIOD_TYPE::VARCHAR(50) as TIME_PERIOD_TYPE,
        DATE_TYPE::VARCHAR(100) as DATE_TYPE,
        LAST_PERIOD::DATE as LAST_PERIOD,
        SUPPLIER::VARCHAR(100) as SUPPLIER,
        SOURCE::VARCHAR(100) as SOURCE,
        PRODUCT::VARCHAR(3000) as PRODUCT,
        SEGMENT::VARCHAR(300) as SEGMENT,
        MANUFACTURER::VARCHAR(300) as MANUFACTURER,
        BRAND::VARCHAR(300) as BRAND,
        SUB_BRAND::VARCHAR(300) as SUB_BRAND,
        PACKSIZE::FLOAT as PACKSIZE,
        NPD::VARCHAR(10) as NPD,
        LAUNCH_YEAR::NUMBER(18,0) as LAUNCH_YEAR,
        BASE_FLAG::VARCHAR(10) as BASE_FLAG,
        EXCLUDE_FLAG::VARCHAR(10) as EXCLUDE_FLAG,
        CAT_UNIT_TYPE::VARCHAR(50) as CAT_UNIT_TYPE,
        CAT_SELLING_UNIT::VARCHAR(50) as CAT_SELLING_UNIT,
        UNIT::VARCHAR(100) as UNIT,
        ATTRIBUTE_1::VARCHAR(200) as ATTRIBUTE_1,
        ATTRIBUTE_1_DESC::VARCHAR(200) as ATTRIBUTE_1_DESC,
        ATTRIBUTE_2::VARCHAR(200) as ATTRIBUTE_2,
        ATTRIBUTE_2_DESC::VARCHAR(200) as ATTRIBUTE_2_DESC,
        ATTRIBUTE_3::VARCHAR(200) as ATTRIBUTE_3,
        ATTRIBUTE_3_DESC::VARCHAR(200) as ATTRIBUTE_3_DESC,
        ATTRIBUTE_4::VARCHAR(200) as ATTRIBUTE_4,
        ATTRIBUTE_4_DESC::VARCHAR(200) as ATTRIBUTE_4_DESC,
        ATTRIBUTE_5::VARCHAR(200) as ATTRIBUTE_5,
        ATTRIBUTE_5_DESC::VARCHAR(200) as ATTRIBUTE_5_DESC,
        ATTRIBUTE_6::VARCHAR(200) as ATTRIBUTE_6,
        ATTRIBUTE_6_DESC::VARCHAR(200) as ATTRIBUTE_6_DESC,
        ATTRIBUTE_7::VARCHAR(200) as ATTRIBUTE_7,
        ATTRIBUTE_7_DESC::VARCHAR(200) as ATTRIBUTE_7_DESC,
        ATTRIBUTE_8::VARCHAR(200) as ATTRIBUTE_8,
        ATTRIBUTE_8_DESC::VARCHAR(200) as ATTRIBUTE_8_DESC,
        ATTRIBUTE_9::VARCHAR(200) as ATTRIBUTE_9,
        ATTRIBUTE_9_DESC::VARCHAR(200) as ATTRIBUTE_9_DESC,
        ATTRIBUTE_10::VARCHAR(200) as ATTRIBUTE_10,
        ATTRIBUTE_10_DESC::VARCHAR(200) as ATTRIBUTE_10_DESC,
        ATTRIBUTE_11::VARCHAR(200) as ATTRIBUTE_11,
        ATTRIBUTE_11_DESC::VARCHAR(200) as ATTRIBUTE_11_DESC,
        ATTRIBUTE_12::VARCHAR(200) as ATTRIBUTE_12,
        ATTRIBUTE_12_DESC::VARCHAR(200) as ATTRIBUTE_12_DESC,
        ATTRIBUTE_13::VARCHAR(200) as ATTRIBUTE_13,
        ATTRIBUTE_13_DESC::VARCHAR(200) as ATTRIBUTE_13_DESC,
        ATTRIBUTE_14::VARCHAR(200) as ATTRIBUTE_14,
        ATTRIBUTE_14_DESC::VARCHAR(200) as ATTRIBUTE_14_DESC,
        ATTRIBUTE_15::VARCHAR(200) as ATTRIBUTE_15,
        ATTRIBUTE_15_DESC::VARCHAR(200) as ATTRIBUTE_15_DESC,
        ATTRIBUTE_16::VARCHAR(200) as ATTRIBUTE_16,
        ATTRIBUTE_16_DESC::VARCHAR(200) as ATTRIBUTE_16_DESC,
        ATTRIBUTE_17::VARCHAR(200) as ATTRIBUTE_17,
        ATTRIBUTE_17_DESC::VARCHAR(200) as ATTRIBUTE_17_DESC,
        GPH_GFO::VARCHAR(250) as GPH_GFO,
        GPH_BRAND::VARCHAR(250) as GPH_BRAND,
        GPH_SUB_BRAND::VARCHAR(250) as GPH_SUB_BRAND,
        GPH_VARIANT::VARCHAR(250) as GPH_VARIANT,
        GPH_NEED_STATE::VARCHAR(250) as GPH_NEED_STATE,
        GPH_CATEGORY::VARCHAR(250) as GPH_CATEGORY,
        GPH_SUB_CATEGORY::VARCHAR(250) as GPH_SUB_CATEGORY,
        GPH_SEGMENT::VARCHAR(250) as GPH_SEGMENT,
        GPH_SUB_SEGMENT::VARCHAR(250) as GPH_SUB_SEGMENT,
        GGH_COUNTRY::VARCHAR(250) as GGH_COUNTRY,
        GGH_REGION::VARCHAR(250) as GGH_REGION,
        GGH_CLUSTER::VARCHAR(250) as GGH_CLUSTER,
        GGH_SUB_CLUSTER::VARCHAR(200) as GGH_SUB_CLUSTER,
        GGH_COUNTRY_3_CD::VARCHAR(250) as GGH_COUNTRY_3_CD,
        GGH_COUNTRY_2_CD::VARCHAR(250) as GGH_COUNTRY_2_CD,
        GGH_MARKET_TYPE::VARCHAR(200) as GGH_MARKET_TYPE,
        SKU_VALUE_SALES_LC::FLOAT as SKU_VALUE_SALES_LC,
        CAT_VALUE_SALES_LC::FLOAT as CAT_VALUE_SALES_LC,
        SEG_VALUE_SALES_LC::FLOAT as SEG_VALUE_SALES_LC,
        MNF_VALUE_SALES_LC::FLOAT as MNF_VALUE_SALES_LC,
        MNF_SEG_VALUE_SALES_LC::FLOAT as MNF_SEG_VALUE_SALES_LC,
        BRD_VALUE_SALES_LC::FLOAT as BRD_VALUE_SALES_LC,
        BRD_SEG_VALUE_SALES_LC::FLOAT as BRD_SEG_VALUE_SALES_LC,
        SKU_VALUE_SALES_USD::FLOAT as SKU_VALUE_SALES_USD,
        CAT_VALUE_SALES_USD::FLOAT as CAT_VALUE_SALES_USD,
        SEG_VALUE_SALES_USD::FLOAT as SEG_VALUE_SALES_USD,
        MNF_VALUE_SALES_USD::FLOAT as MNF_VALUE_SALES_USD,
        MNF_SEG_VALUE_SALES_USD::FLOAT as MNF_SEG_VALUE_SALES_USD,
        BRD_VALUE_SALES_USD::FLOAT as BRD_VALUE_SALES_USD,
        BRD_SEG_VALUE_SALES_USD::FLOAT as BRD_SEG_VALUE_SALES_USD,
        SKU_PROMOTED_VALUE_SALES_LC::FLOAT as SKU_PROMOTED_VALUE_SALES_LC,
        CAT_PROMOTED_VALUE_SALES_LC::FLOAT as CAT_PROMOTED_VALUE_SALES_LC,
        SEG_PROMOTED_VALUE_SALES_LC::FLOAT as SEG_PROMOTED_VALUE_SALES_LC,
        MNF_PROMOTED_VALUE_SALES_LC::FLOAT as MNF_PROMOTED_VALUE_SALES_LC,
        MNF_SEG_PROMOTED_VALUE_SALES_LC::FLOAT as MNF_SEG_PROMOTED_VALUE_SALES_LC,
        BRD_PROMOTED_VALUE_SALES_LC::FLOAT as BRD_PROMOTED_VALUE_SALES_LC,
        BRD_SEG_PROMOTED_VALUE_SALES_LC::FLOAT as BRD_SEG_PROMOTED_VALUE_SALES_LC,
        SKU_PROMOTED_VALUE_SALES_USD::FLOAT as SKU_PROMOTED_VALUE_SALES_USD,
        CAT_PROMOTED_VALUE_SALES_USD::FLOAT as CAT_PROMOTED_VALUE_SALES_USD,
        SEG_PROMOTED_VALUE_SALES_USD::FLOAT as SEG_PROMOTED_VALUE_SALES_USD,
        MNF_PROMOTED_VALUE_SALES_USD::FLOAT as MNF_PROMOTED_VALUE_SALES_USD,
        MNF_SEG_PROMOTED_VALUE_SALES_USD::FLOAT as MNF_SEG_PROMOTED_VALUE_SALES_USD,
        BRD_PROMOTED_VALUE_SALES_USD::FLOAT as BRD_PROMOTED_VALUE_SALES_USD,
        BRD_SEG_PROMOTED_VALUE_SALES_USD::FLOAT as BRD_SEG_PROMOTED_VALUE_SALES_USD,
        SKU_BASE_VALUE_SALES_LC::FLOAT as SKU_BASE_VALUE_SALES_LC,
        CAT_BASE_VALUE_SALES_LC::FLOAT as CAT_BASE_VALUE_SALES_LC,
        SEG_BASE_VALUE_SALES_LC::FLOAT as SEG_BASE_VALUE_SALES_LC,
        MNF_BASE_VALUE_SALES_LC::FLOAT as MNF_BASE_VALUE_SALES_LC,
        MNF_SEG_BASE_VALUE_SALES_LC::FLOAT as MNF_SEG_BASE_VALUE_SALES_LC,
        BRD_BASE_VALUE_SALES_LC::FLOAT as BRD_BASE_VALUE_SALES_LC,
        BRD_SEG_BASE_VALUE_SALES_LC::FLOAT as BRD_SEG_BASE_VALUE_SALES_LC,
        SKU_BASE_VALUE_SALES_USD::FLOAT as SKU_BASE_VALUE_SALES_USD,
        CAT_BASE_VALUE_SALES_USD::FLOAT as CAT_BASE_VALUE_SALES_USD,
        SEG_BASE_VALUE_SALES_USD::FLOAT as SEG_BASE_VALUE_SALES_USD,
        MNF_BASE_VALUE_SALES_USD::FLOAT as MNF_BASE_VALUE_SALES_USD,
        MNF_SEG_BASE_VALUE_SALES_USD::FLOAT as MNF_SEG_BASE_VALUE_SALES_USD,
        BRD_BASE_VALUE_SALES_USD::FLOAT as BRD_BASE_VALUE_SALES_USD,
        BRD_SEG_BASE_VALUE_SALES_USD::FLOAT as BRD_SEG_BASE_VALUE_SALES_USD,
        SKU_UNIT_SALES::FLOAT as SKU_UNIT_SALES,
        CAT_UNIT_SALES::FLOAT as CAT_UNIT_SALES,
        SEG_UNIT_SALES::FLOAT as SEG_UNIT_SALES,
        MNF_UNIT_SALES::FLOAT as MNF_UNIT_SALES,
        MNF_SEG_UNIT_SALES::FLOAT as MNF_SEG_UNIT_SALES,
        BRD_UNIT_SALES::FLOAT as BRD_UNIT_SALES,
        BRD_SEG_UNIT_SALES::FLOAT as BRD_SEG_UNIT_SALES,
        SKU_PROMOTED_UNIT_SALES::FLOAT as SKU_PROMOTED_UNIT_SALES,
        CAT_PROMOTED_UNIT_SALES::FLOAT as CAT_PROMOTED_UNIT_SALES,
        SEG_PROMOTED_UNIT_SALES::FLOAT as SEG_PROMOTED_UNIT_SALES,
        MNF_PROMOTED_UNIT_SALES::FLOAT as MNF_PROMOTED_UNIT_SALES,
        MNF_SEG_PROMOTED_UNIT_SALES::FLOAT as MNF_SEG_PROMOTED_UNIT_SALES,
        BRD_PROMOTED_UNIT_SALES::FLOAT as BRD_PROMOTED_UNIT_SALES,
        BRD_SEG_PROMOTED_UNIT_SALES::FLOAT as BRD_SEG_PROMOTED_UNIT_SALES,
        SKU_BASE_UNIT_SALES::FLOAT as SKU_BASE_UNIT_SALES,
        CAT_BASE_UNIT_SALES::FLOAT as CAT_BASE_UNIT_SALES,
        SEG_BASE_UNIT_SALES::FLOAT as SEG_BASE_UNIT_SALES,
        MNF_BASE_UNIT_SALES::FLOAT as MNF_BASE_UNIT_SALES,
        MNF_SEG_BASE_UNIT_SALES::FLOAT as MNF_SEG_BASE_UNIT_SALES,
        BRD_BASE_UNIT_SALES::FLOAT as BRD_BASE_UNIT_SALES,
        BRD_SEG_BASE_UNIT_SALES::FLOAT as BRD_SEG_BASE_UNIT_SALES,
        SKU_VOLUME_SALES::FLOAT as SKU_VOLUME_SALES,
        CAT_VOLUME_SALES::FLOAT as CAT_VOLUME_SALES,
        SEG_VOLUME_SALES::FLOAT as SEG_VOLUME_SALES,
        MNF_VOLUME_SALES::FLOAT as MNF_VOLUME_SALES,
        MNF_SEG_VOLUME_SALES::FLOAT as MNF_SEG_VOLUME_SALES,
        BRD_VOLUME_SALES::FLOAT as BRD_VOLUME_SALES,
        BRD_SEG_VOLUME_SALES::FLOAT as BRD_SEG_VOLUME_SALES,
        SKU_PROMOTED_VOLUME_SALES::FLOAT as SKU_PROMOTED_VOLUME_SALES,
        CAT_PROMOTED_VOLUME_SALES::FLOAT as CAT_PROMOTED_VOLUME_SALES,
        SEG_PROMOTED_VOLUME_SALES::FLOAT as SEG_PROMOTED_VOLUME_SALES,
        MNF_PROMOTED_VOLUME_SALES::FLOAT as MNF_PROMOTED_VOLUME_SALES,
        MNF_SEG_PROMOTED_VOLUME_SALES::FLOAT as MNF_SEG_PROMOTED_VOLUME_SALES,
        BRD_PROMOTED_VOLUME_SALES::FLOAT as BRD_PROMOTED_VOLUME_SALES,
        BRD_SEG_PROMOTED_VOLUME_SALES::FLOAT as BRD_SEG_PROMOTED_VOLUME_SALES,
        SKU_BASE_VOLUME_SALES::FLOAT as SKU_BASE_VOLUME_SALES,
        CAT_BASE_VOLUME_SALES::FLOAT as CAT_BASE_VOLUME_SALES,
        SEG_BASE_VOLUME_SALES::FLOAT as SEG_BASE_VOLUME_SALES,
        MNF_BASE_VOLUME_SALES::FLOAT as MNF_BASE_VOLUME_SALES,
        MNF_SEG_BASE_VOLUME_SALES::FLOAT as MNF_SEG_BASE_VOLUME_SALES,
        BRD_BASE_VOLUME_SALES::FLOAT as BRD_BASE_VOLUME_SALES,
        BRD_SEG_BASE_VOLUME_SALES::FLOAT as BRD_SEG_BASE_VOLUME_SALES,
        SKU_NUMERIC_DISTRIBUTION::FLOAT as SKU_NUMERIC_DISTRIBUTION,
        CAT_NUMERIC_DISTRIBUTION::FLOAT as CAT_NUMERIC_DISTRIBUTION,
        SEG_NUMERIC_DISTRIBUTION::FLOAT as SEG_NUMERIC_DISTRIBUTION,
        MNF_NUMERIC_DISTRIBUTION::FLOAT as MNF_NUMERIC_DISTRIBUTION,
        MNF_SEG_NUMERIC_DISTRIBUTION::FLOAT as MNF_SEG_NUMERIC_DISTRIBUTION,
        BRD_NUMERIC_DISTRIBUTION::FLOAT as BRD_NUMERIC_DISTRIBUTION,
        BRD_SEG_NUMERIC_DISTRIBUTION::FLOAT as BRD_SEG_NUMERIC_DISTRIBUTION,
        SKU_WEIGHTED_DISTRIBUTION::FLOAT as SKU_WEIGHTED_DISTRIBUTION,
        CAT_WEIGHTED_DISTRIBUTION::FLOAT as CAT_WEIGHTED_DISTRIBUTION,
        SEG_WEIGHTED_DISTRIBUTION::FLOAT as SEG_WEIGHTED_DISTRIBUTION,
        MNF_WEIGHTED_DISTRIBUTION::FLOAT as MNF_WEIGHTED_DISTRIBUTION,
        MNF_SEG_WEIGHTED_DISTRIBUTION::FLOAT as MNF_SEG_WEIGHTED_DISTRIBUTION,
        BRD_WEIGHTED_DISTRIBUTION::FLOAT as BRD_WEIGHTED_DISTRIBUTION,
        BRD_SEG_WEIGHTED_DISTRIBUTION::FLOAT as BRD_SEG_WEIGHTED_DISTRIBUTION,
        SKU_CWD_FEATURE_DISPLAY_PCT::FLOAT as SKU_CWD_FEATURE_DISPLAY_PCT,
        CAT_CWD_FEATURE_DISPLAY_PCT::FLOAT as CAT_CWD_FEATURE_DISPLAY_PCT,
        SEG_CWD_FEATURE_DISPLAY_PCT::FLOAT as SEG_CWD_FEATURE_DISPLAY_PCT,
        MNF_CWD_FEATURE_DISPLAY_PCT::FLOAT as MNF_CWD_FEATURE_DISPLAY_PCT,
        MNF_SEG_CWD_FEATURE_DISPLAY_PCT::FLOAT as MNF_SEG_CWD_FEATURE_DISPLAY_PCT,
        BRD_CWD_FEATURE_DISPLAY_PCT::FLOAT as BRD_CWD_FEATURE_DISPLAY_PCT,
        BRD_SEG_CWD_FEATURE_DISPLAY_PCT::FLOAT as BRD_SEG_CWD_FEATURE_DISPLAY_PCT,
        SKU_CWD_DISPLAY_PCT::FLOAT as SKU_CWD_DISPLAY_PCT,
        CAT_CWD_DISPLAY_PCT::FLOAT as CAT_CWD_DISPLAY_PCT,
        SEG_CWD_DISPLAY_PCT::FLOAT as SEG_CWD_DISPLAY_PCT,
        MNF_CWD_DISPLAY_PCT::FLOAT as MNF_CWD_DISPLAY_PCT,
        MNF_SEG_CWD_DISPLAY_PCT::FLOAT as MNF_SEG_CWD_DISPLAY_PCT,
        BRD_CWD_DISPLAY_PCT::FLOAT as BRD_CWD_DISPLAY_PCT,
        BRD_SEG_CWD_DISPLAY_PCT::FLOAT as BRD_SEG_CWD_DISPLAY_PCT,
        SKU_CWD_FEATURE_PCT::FLOAT as SKU_CWD_FEATURE_PCT,
        CAT_CWD_FEATURE_PCT::FLOAT as CAT_CWD_FEATURE_PCT,
        SEG_CWD_FEATURE_PCT::FLOAT as SEG_CWD_FEATURE_PCT,
        MNF_CWD_FEATURE_PCT::FLOAT as MNF_CWD_FEATURE_PCT,
        MNF_SEG_CWD_FEATURE_PCT::FLOAT as MNF_SEG_CWD_FEATURE_PCT,
        BRD_CWD_FEATURE_PCT::FLOAT as BRD_CWD_FEATURE_PCT,
        BRD_SEG_CWD_FEATURE_PCT::FLOAT as BRD_SEG_CWD_FEATURE_PCT,
        MSL::VARCHAR(10) as MSL,
        FILENAME::VARCHAR(200) as FILENAME,
        FILE_TIME_STAMP::TIMESTAMP_NTZ(9) as FILE_TIME_STAMP,
        LCL_PROD_HIER_LVL_1::VARCHAR(200) as LCL_PROD_HIER_LVL_1,
        LCL_PROD_HIER_LVL_2::VARCHAR(200) as LCL_PROD_HIER_LVL_2,
        LCL_PROD_HIER_LVL_3::VARCHAR(200) as LCL_PROD_HIER_LVL_3,
        LCL_PROD_HIER_LVL_4::VARCHAR(200) as LCL_PROD_HIER_LVL_4,
        LCL_PROD_HIER_LVL_5::VARCHAR(200) as LCL_PROD_HIER_LVL_5,
        LCL_PROD_HIER_LVL_6::VARCHAR(200) as LCL_PROD_HIER_LVL_6,
        LCL_PROD_HIER_LVL_7::VARCHAR(200) as LCL_PROD_HIER_LVL_7,
        LCL_PROD_ATTRIBUTE_1::VARCHAR(200) as LCL_PROD_ATTRIBUTE_1,
        LCL_PROD_ATTRIBUTE_2::VARCHAR(200) as LCL_PROD_ATTRIBUTE_2,
        LCL_PROD_ATTRIBUTE_3::VARCHAR(200) as LCL_PROD_ATTRIBUTE_3,
        LCL_PROD_ATTRIBUTE_4::VARCHAR(200) as LCL_PROD_ATTRIBUTE_4,
        LCL_PROD_ATTRIBUTE_5::VARCHAR(200) as LCL_PROD_ATTRIBUTE_5,
        DELIVERY_STATUS::VARCHAR(50) as DELIVERY_STATUS,
        KEY::VARCHAR(50) as KEY,
        ORIGIN::VARCHAR(50) as ORIGIN,
        GSR_DESCRIPTION::VARCHAR(300) as GSR_DESCRIPTION,
        GSR_LEVEL::VARCHAR(100) as GSR_LEVEL,
        GSR_FLAG::BOOLEAN as GSR_FLAG,
        LAUNCH_DATE::DATE as LAUNCH_DATE,
        SKU_CWD_ANY_PROMO_PCT::FLOAT as SKU_CWD_ANY_PROMO_PCT,
        CAT_CWD_ANY_PROMO_PCT::FLOAT as CAT_CWD_ANY_PROMO_PCT,
        SEG_CWD_ANY_PROMO_PCT::FLOAT as SEG_CWD_ANY_PROMO_PCT,
        MNF_CWD_ANY_PROMO_PCT::FLOAT as MNF_CWD_ANY_PROMO_PCT,
        MNF_SEG_CWD_ANY_PROMO_PCT::FLOAT as MNF_SEG_CWD_ANY_PROMO_PCT,
        BRD_CWD_ANY_PROMO_PCT::FLOAT as BRD_CWD_ANY_PROMO_PCT,
        BRD_SEG_CWD_ANY_PROMO_PCT::FLOAT as BRD_SEG_CWD_ANY_PROMO_PCT,
        SKU_CWD_PRICE_PROMO_PCT::FLOAT as SKU_CWD_PRICE_PROMO_PCT,
        CAT_CWD_PRICE_PROMO_PCT::FLOAT as CAT_CWD_PRICE_PROMO_PCT,
        SEG_CWD_PRICE_PROMO_PCT::FLOAT as SEG_CWD_PRICE_PROMO_PCT,
        MNF_CWD_PRICE_PROMO_PCT::FLOAT as MNF_CWD_PRICE_PROMO_PCT,
        MNF_SEG_CWD_PRICE_PROMO_PCT::FLOAT as MNF_SEG_CWD_PRICE_PROMO_PCT,
        BRD_CWD_PRICE_PROMO_PCT::FLOAT as BRD_CWD_PRICE_PROMO_PCT,
        BRD_SEG_CWD_PRICE_PROMO_PCT::FLOAT as BRD_SEG_CWD_PRICE_PROMO_PCT,
        SKU_CWD_EXTRA_QTY_PCT::FLOAT as SKU_CWD_EXTRA_QTY_PCT,
        CAT_CWD_EXTRA_QTY_PCT::FLOAT as CAT_CWD_EXTRA_QTY_PCT,
        SEG_CWD_EXTRA_QTY_PCT::FLOAT as SEG_CWD_EXTRA_QTY_PCT,
        MNF_CWD_EXTRA_QTY_PCT::FLOAT as MNF_CWD_EXTRA_QTY_PCT,
        MNF_SEG_CWD_EXTRA_QTY_PCT::FLOAT as MNF_SEG_CWD_EXTRA_QTY_PCT,
        BRD_CWD_EXTRA_QTY_PCT::FLOAT as BRD_CWD_EXTRA_QTY_PCT,
        BRD_SEG_CWD_EXTRA_QTY_PCT::FLOAT as BRD_SEG_CWD_EXTRA_QTY_PCT,
        SKU_CWD_DISPLAY_OTHER_PCT::FLOAT as SKU_CWD_DISPLAY_OTHER_PCT,
        CAT_CWD_DISPLAY_OTHER_PCT::FLOAT as CAT_CWD_DISPLAY_OTHER_PCT,
        SEG_CWD_DISPLAY_OTHER_PCT::FLOAT as SEG_CWD_DISPLAY_OTHER_PCT,
        MNF_CWD_DISPLAY_OTHER_PCT::FLOAT as MNF_CWD_DISPLAY_OTHER_PCT,
        MNF_SEG_CWD_DISPLAY_OTHER_PCT::FLOAT as MNF_SEG_CWD_DISPLAY_OTHER_PCT,
        BRD_CWD_DISPLAY_OTHER_PCT::FLOAT as BRD_CWD_DISPLAY_OTHER_PCT,
        BRD_SEG_CWD_DISPLAY_OTHER_PCT::FLOAT as BRD_SEG_CWD_DISPLAY_OTHER_PCT,
        SKU_CWD_DISPLAY_ENTRY_PCT::FLOAT as SKU_CWD_DISPLAY_ENTRY_PCT,
        CAT_CWD_DISPLAY_ENTRY_PCT::FLOAT as CAT_CWD_DISPLAY_ENTRY_PCT,
        SEG_CWD_DISPLAY_ENTRY_PCT::FLOAT as SEG_CWD_DISPLAY_ENTRY_PCT,
        MNF_CWD_DISPLAY_ENTRY_PCT::FLOAT as MNF_CWD_DISPLAY_ENTRY_PCT,
        MNF_SEG_CWD_DISPLAY_ENTRY_PCT::FLOAT as MNF_SEG_CWD_DISPLAY_ENTRY_PCT,
        BRD_CWD_DISPLAY_ENTRY_PCT::FLOAT as BRD_CWD_DISPLAY_ENTRY_PCT,
        BRD_SEG_CWD_DISPLAY_ENTRY_PCT::FLOAT as BRD_SEG_CWD_DISPLAY_ENTRY_PCT,
        SKU_CWD_DISPLAY_GONDOLA_PCT::FLOAT as SKU_CWD_DISPLAY_GONDOLA_PCT,
        CAT_CWD_DISPLAY_GONDOLA_PCT::FLOAT as CAT_CWD_DISPLAY_GONDOLA_PCT,
        SEG_CWD_DISPLAY_GONDOLA_PCT::FLOAT as SEG_CWD_DISPLAY_GONDOLA_PCT,
        MNF_CWD_DISPLAY_GONDOLA_PCT::FLOAT as MNF_CWD_DISPLAY_GONDOLA_PCT,
        MNF_SEG_CWD_DISPLAY_GONDOLA_PCT::FLOAT as MNF_SEG_CWD_DISPLAY_GONDOLA_PCT,
        BRD_CWD_DISPLAY_GONDOLA_PCT::FLOAT as BRD_CWD_DISPLAY_GONDOLA_PCT,
        BRD_SEG_CWD_DISPLAY_GONDOLA_PCT::FLOAT as BRD_SEG_CWD_DISPLAY_GONDOLA_PCT,
        SKU_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as SKU_CWD_DISPLAY_CHECK_OUT_PCT,
        CAT_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as CAT_CWD_DISPLAY_CHECK_OUT_PCT,
        SEG_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as SEG_CWD_DISPLAY_CHECK_OUT_PCT,
        MNF_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as MNF_CWD_DISPLAY_CHECK_OUT_PCT,
        MNF_SEG_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as MNF_SEG_CWD_DISPLAY_CHECK_OUT_PCT,
        BRD_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as BRD_CWD_DISPLAY_CHECK_OUT_PCT,
        BRD_SEG_CWD_DISPLAY_CHECK_OUT_PCT::FLOAT as BRD_SEG_CWD_DISPLAY_CHECK_OUT_PCT,
        SKU_CWD_MULTIBUY_PCT::FLOAT as SKU_CWD_MULTIBUY_PCT,
        CAT_CWD_MULTIBUY_PCT::FLOAT as CAT_CWD_MULTIBUY_PCT,
        SEG_CWD_MULTIBUY_PCT::FLOAT as SEG_CWD_MULTIBUY_PCT,
        MNF_CWD_MULTIBUY_PCT::FLOAT as MNF_CWD_MULTIBUY_PCT,
        MNF_SEG_CWD_MULTIBUY_PCT::FLOAT as MNF_SEG_CWD_MULTIBUY_PCT,
        BRD_CWD_MULTIBUY_PCT::FLOAT as BRD_CWD_MULTIBUY_PCT,
        BRD_SEG_CWD_MULTIBUY_PCT::FLOAT as BRD_SEG_CWD_MULTIBUY_PCT,
        SKU_CWD_LOYALTY_CARD_PCT::FLOAT as SKU_CWD_LOYALTY_CARD_PCT,
        CAT_CWD_LOYALTY_CARD_PCT::FLOAT as CAT_CWD_LOYALTY_CARD_PCT,
        SEG_CWD_LOYALTY_CARD_PCT::FLOAT as SEG_CWD_LOYALTY_CARD_PCT,
        MNF_CWD_LOYALTY_CARD_PCT::FLOAT as MNF_CWD_LOYALTY_CARD_PCT,
        MNF_SEG_CWD_LOYALTY_CARD_PCT::FLOAT as MNF_SEG_CWD_LOYALTY_CARD_PCT,
        BRD_CWD_LOYALTY_CARD_PCT::FLOAT as BRD_CWD_LOYALTY_CARD_PCT,
        BRD_SEG_CWD_LOYALTY_CARD_PCT::FLOAT as BRD_SEG_CWD_LOYALTY_CARD_PCT,
        CHANNEL_ORIGIN::VARCHAR(50) as CHANNEL_ORIGIN,
        SKU_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as SKU_WEIGHTED_DISTRIBUTION_W_PCT,
        CAT_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as CAT_WEIGHTED_DISTRIBUTION_W_PCT,
        SEG_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as SEG_WEIGHTED_DISTRIBUTION_W_PCT,
        MNF_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as MNF_WEIGHTED_DISTRIBUTION_W_PCT,
        MNF_SEG_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as MNF_SEG_WEIGHTED_DISTRIBUTION_W_PCT,
        BRD_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as BRD_WEIGHTED_DISTRIBUTION_W_PCT,
        BRD_SEG_WEIGHTED_DISTRIBUTION_W_PCT::FLOAT as BRD_SEG_WEIGHTED_DISTRIBUTION_W_PCT,
        SKU_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as SKU_WEIGHTED_DISTRIBUTION_4W_PCT,
        CAT_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as CAT_WEIGHTED_DISTRIBUTION_4W_PCT,
        SEG_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as SEG_WEIGHTED_DISTRIBUTION_4W_PCT,
        MNF_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as MNF_WEIGHTED_DISTRIBUTION_4W_PCT,
        MNF_SEG_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as MNF_SEG_WEIGHTED_DISTRIBUTION_4W_PCT,
        BRD_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as BRD_WEIGHTED_DISTRIBUTION_4W_PCT,
        BRD_SEG_WEIGHTED_DISTRIBUTION_4W_PCT::FLOAT as BRD_SEG_WEIGHTED_DISTRIBUTION_4W_PCT,
        SKU_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as SKU_WEIGHTED_DISTRIBUTION_12W_PCT,
        CAT_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as CAT_WEIGHTED_DISTRIBUTION_12W_PCT,
        SEG_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as SEG_WEIGHTED_DISTRIBUTION_12W_PCT,
        MNF_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as MNF_WEIGHTED_DISTRIBUTION_12W_PCT,
        MNF_SEG_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as MNF_SEG_WEIGHTED_DISTRIBUTION_12W_PCT,
        BRD_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as BRD_WEIGHTED_DISTRIBUTION_12W_PCT,
        BRD_SEG_WEIGHTED_DISTRIBUTION_12W_PCT::FLOAT as BRD_SEG_WEIGHTED_DISTRIBUTION_12W_PCT,
        SKU_WEIGHTED_DISTRIBUTION_PCT::FLOAT as SKU_WEIGHTED_DISTRIBUTION_PCT,
        CAT_WEIGHTED_DISTRIBUTION_PCT::FLOAT as CAT_WEIGHTED_DISTRIBUTION_PCT,
        SEG_WEIGHTED_DISTRIBUTION_PCT::FLOAT as SEG_WEIGHTED_DISTRIBUTION_PCT,
        MNF_WEIGHTED_DISTRIBUTION_PCT::FLOAT as MNF_WEIGHTED_DISTRIBUTION_PCT,
        MNF_SEG_WEIGHTED_DISTRIBUTION_PCT::FLOAT as MNF_SEG_WEIGHTED_DISTRIBUTION_PCT,
        BRD_WEIGHTED_DISTRIBUTION_PCT::FLOAT as BRD_WEIGHTED_DISTRIBUTION_PCT,
        BRD_SEG_WEIGHTED_DISTRIBUTION_PCT::FLOAT as BRD_SEG_WEIGHTED_DISTRIBUTION_PCT,
        MANUFACTURER_LOCAL::VARCHAR(300) as MANUFACTURER_LOCAL,
        MANUFACTURER_GLOBAL::VARCHAR(300) as MANUFACTURER_GLOBAL,
        BRAND_LOCAL::VARCHAR(300) as BRAND_LOCAL,
        BRAND_GLOBAL::VARCHAR(300) as BRAND_GLOBAL,
        NEED_STATE::VARCHAR(100) as NEED_STATE,
        GFO::VARCHAR(100) as GFO,
        CLUSTER::VARCHAR(100) as CLUSTER,
        SUB_CLUSTER::VARCHAR(100) as SUB_CLUSTER,
        FORM::VARCHAR(500) as FORM
    from result
)

select * from final