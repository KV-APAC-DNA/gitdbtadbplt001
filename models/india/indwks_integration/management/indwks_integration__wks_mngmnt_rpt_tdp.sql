with edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_sku_recom_spike_msl as(
    select * from {{ ref('indedw_integration__edw_sku_recom_spike_msl') }}
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
        ,pd.mothersku_name
        ,COUNT(DISTINCT cust_cd||retailer_cd||salesman_code) AS total_stores
        ,SUM(sku.ms_flag) AS total_recos
        ,SUM(sku.hit_ms_flag) AS total_hits
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
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
select * from transformed