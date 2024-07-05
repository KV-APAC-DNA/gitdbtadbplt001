with edw_sku_recom_spike_msl as(
    select * from {{ ref('indedw_integration__edw_sku_recom_spike_msl') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),

transformed as(    
    SELECT sku.mth_mm
        ,sku.region_name
        ,sku.zone_name
        ,sku.territory_name
        ,sku.channel_name
        ,sku.class_desc
        ,sku.retailer_channel_level_3
        ,pd.franchise_name
        ,pd.brand_name
        ,pd.product_category_name
        ,pd.variant_name
        ,SUM(sku.achievement_nr_val) AS achievement_nr_msl_msku
    FROM edw_sku_recom_spike_msl sku
    LEFT JOIN (SELECT franchise_name,
                    brand_name,
                    product_category_name,
                    variant_name,
                    mothersku_name,
                    mothersku_code
            FROM edw_product_dim
            WHERE NVL(delete_flag,'XYZ') <> 'N'
            GROUP BY 1, 2, 3, 4, 5, 6) pd
        ON sku.mother_sku_cd = pd.mothersku_code
    WHERE sku.hit_ms_flag = 1
    AND sku.business_channel = 'GT'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
select * from transformed