WITH wks_mi_msl_lkp_mnthly_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_lkp_mnthly_tbl') }}
),
wks_mi_msl_sales_vs_reco_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_sales_vs_reco_tbl') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
sales_ret_sku_list AS (
    SELECT msl.*,
        sales.customer_code,
        sales.customer_name,
        sales.retailer_code,
        sales.retailer_name,
        sales.rtruniquecode,
        sales.channel_name,
        sales.retailer_category_name,
        sales.salesman_code,
        sales.salesman_name,
        sales.unique_sales_code,
        sales.territory_name,
        sales.latest_customer_code,
        sales.latest_customer_name,
        sales.latest_territory_code,
        sales.latest_territory_name,
        sales.latest_salesman_code,
        sales.latest_salesman_name,
        sales.latest_uniquesalescode,
        sales.total_subd
    FROM wks_mi_msl_lkp_mnthly_tbl msl
    LEFT JOIN (
        SELECT mth_mm,
            customer_code,
            customer_name,
            retailer_code,
            retailer_name,
            rtruniquecode,
            region_name,
            zone_name,
            territory_name,
            channel_name,
            retailer_category_name,
            salesman_code,
            salesman_name,
            unique_sales_code,
            latest_customer_code,
            latest_customer_name,
            latest_territory_code,
            latest_territory_name,
            latest_salesman_code,
            latest_salesman_name,
            latest_uniquesalescode,
            total_subd
        FROM wks_mi_msl_sales_vs_reco_tbl
        GROUP BY mth_mm,
            customer_code,
            customer_name,
            retailer_code,
            retailer_name,
            rtruniquecode,
            region_name,
            zone_name,
            territory_name,
            channel_name,
            retailer_category_name,
            salesman_code,
            salesman_name,
            unique_sales_code,
            latest_customer_code,
            latest_customer_name,
            latest_territory_code,
            latest_territory_name,
            latest_salesman_code,
            latest_salesman_name,
            latest_uniquesalescode,
            total_subd
        ) sales ON msl.mth_mm = sales.mth_mm
        AND UPPER(TRIM(msl.region_name)) = UPPER(TRIM(sales.region_name))
        AND UPPER(TRIM(msl.zone_name)) = UPPER(TRIM(sales.zone_name))
),
transformed as(
     SELECT reco.mth_mm,
    reco.customer_code,
    reco.customer_name,
    reco.retailer_code,
    reco.retailer_name,
    reco.rtruniquecode,
    reco.region_name,
    reco.zone_name,
    reco.territory_name,
    reco.channel_name,
    reco.retailer_category_name,
    reco.salesman_code,
    reco.salesman_name,
    reco.unique_sales_code,
    reco.latest_customer_code,
    reco.latest_customer_name,
    reco.latest_territory_code,
    reco.latest_territory_name,
    reco.latest_salesman_code,
    reco.latest_salesman_name,
    reco.latest_uniquesalescode,
    reco.total_subd,
    pd1.mothersku_code AS mothersku_code_recom,
    reco.mothersku_name AS mothersku_name_recom,
    sold.mothersku_code_sold,
    sold.mothersku_name_sold,
    '1' AS ms_flag,
    CASE 
        WHEN sold.achievement_nr > 0
            THEN '1'
        ELSE '0'
        END AS hit_ms_flag,
    SUM(sold.quantity) AS quantity,
    SUM(sold.achievement_nr) AS achievement_nr,
    --MAX(reco.total_subd) AS total_subd,
    reco.qtr,
    reco.period,
    MAX(sold.msl_count) AS msl_count,
    COUNT(DISTINCT sold.mothersku_name_sold) AS msl_sold_count FROM sales_ret_sku_list reco LEFT JOIN wks_mi_msl_sales_vs_reco_tbl sold ON UPPER(TRIM(reco.customer_code)) = UPPER(TRIM(sold.customer_code))
    AND UPPER(TRIM(reco.retailer_code)) = UPPER(TRIM(sold.retailer_code))
    AND UPPER(TRIM(reco.mothersku_name)) = UPPER(TRIM(sold.mothersku_name_sold))
    AND reco.mth_mm = sold.mth_mm LEFT JOIN (
    SELECT mothersku_code,
        mothersku_name
    FROM edw_product_dim
    WHERE NVL(delete_flag, 'XYZ') <> 'N'
    GROUP BY 1,
        2
    ) pd1 ON UPPER(TRIM(reco.mothersku_name)) = UPPER(TRIM(pd1.mothersku_name)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 31, 32
)
select * from transformed