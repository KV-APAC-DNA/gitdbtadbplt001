with wks_skurecom_mi_actuals_itg_flag as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_itg_flag') }}
),
edw_customer_dim as(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
wks_skurecom_mi_actuals_retdim as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_retdim') }}
),
itg_in_mds_channel_mapping as(
    select * from {{ ref('inditg_integration__itg_in_mds_channel_mapping') }}
),
final as(
    SELECT sku.year,
       sku.quarter,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       cud.customer_name,
       cud.region_name,
       cud.zone_name,
       cud.territory_name,
       rd.rtruniquecode,
       rd.class_desc,
       rd.channel_name,
       rd.business_channel,
       rd.status_desc,
       rd.actv_flg,
       rd.retailer_name,
       rd.retailer_category_name,
       rd.csrtrcode,
       ------------------
       COALESCE(CASE WHEN cmap.retailer_channel_level_1::TEXT = '' THEN NULL
                     ELSE cmap.retailer_channel_level_1 
                END,'Unknown') AS retailer_channel_level_1,
       COALESCE(CASE WHEN cmap.retailer_channel_level_2::TEXT = '' THEN NULL
                     ELSE cmap.retailer_channel_level_2 
                END,'Unknown') AS retailer_channel_level_2,
       COALESCE(CASE WHEN cmap.retailer_channel_level_3::TEXT = '' THEN NULL
                     ELSE cmap.retailer_channel_level_3
                END,'Unknown') AS retailer_channel_level_3
FROM wks_skurecom_mi_actuals_itg_flag sku
LEFT JOIN edw_customer_dim cud
       ON COALESCE(cud.customer_code,'0')::TEXT = COALESCE(sku.cust_cd,'0')::TEXT
LEFT JOIN wks_skurecom_mi_actuals_retdim rd
       ON COALESCE (rd.retailer_code,'0')::TEXT = COALESCE (sku.retailer_cd,'0')::TEXT
      AND COALESCE (rd.customer_code,'0')::TEXT = COALESCE (sku.cust_cd,'0')::TEXT
LEFT JOIN itg_in_mds_channel_mapping cmap
       ON cmap.channel_name::TEXT = CASE WHEN CASE WHEN rd.channel_name::TEXT = ''THEN NULL
                                                   ELSE rd.channel_name END IS NULL THEN 'Unknown'
                                        ELSE rd.channel_name END::TEXT 
      AND cmap.retailer_category_name::TEXT = CASE WHEN CASE WHEN rd.retailer_category_name::TEXT = ''THEN NULL
                                                             ELSE rd.retailer_category_name END IS NULL THEN 'Unknown'
                                                   ELSE rd.retailer_category_name END::TEXT
      AND cmap.retailer_class::TEXT = CASE WHEN CASE WHEN rd.class_desc::TEXT = ''THEN NULL
                                                     ELSE rd.class_desc END IS NULL THEN 'Unknown'
                                           ELSE rd.class_desc END::TEXT
      AND cmap.territory_classification::TEXT = CASE WHEN CASE WHEN rd.territory_classification::TEXT = ''THEN NULL
                                                               ELSE rd.territory_classification END IS NULL THEN 'Unknown'
                                                     ELSE rd.territory_classification END::TEXT 
)
select * from final


