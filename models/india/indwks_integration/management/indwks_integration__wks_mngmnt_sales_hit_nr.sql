with wks_mngmnt_sales_achnr as(
    select * from DEV_DNA_CORE.snapINDwks_INTEGRATION.wks_mngmnt_sales_achnr
),
wks_mngmnt_hit_achnr as(
    select * from DEV_DNA_CORE.snapINDwks_INTEGRATION.wks_mngmnt_hit_achnr
),
transformed as(
    SELECT all_sales.mth_mm
        ,all_sales.region_name
        ,all_sales.zone_name
        ,all_sales.territory_name
        ,all_sales.channel_name
        ,all_sales.class_desc
        ,all_sales.retailer_channel_level_3
        ,all_sales.franchise_name
        ,all_sales.brand_name
        ,all_sales.product_category_name
        ,all_sales.variant_name
        ,all_sales.achievement_nr_all_msku
        ,msl_sales.achievement_nr_msl_msku
    FROM wks_mngmnt_sales_achnr all_sales
    LEFT JOIN wks_mngmnt_hit_achnr msl_sales
        ON  all_sales.mth_mm = msl_sales.mth_mm
        AND all_sales.region_name = msl_sales.region_name
        AND all_sales.zone_name = msl_sales.zone_name
        AND all_sales.territory_name = msl_sales.territory_name
        AND all_sales.channel_name = msl_sales.channel_name
        AND all_sales.class_desc = msl_sales.class_desc
        AND all_sales.retailer_channel_level_3 = msl_sales.retailer_channel_level_3
        AND all_sales.franchise_name = msl_sales.franchise_name
        AND all_sales.brand_name = msl_sales.brand_name
        AND all_sales.product_category_name = msl_sales.product_category_name
        AND all_sales.variant_name = msl_sales.variant_name
)
select * from transformed