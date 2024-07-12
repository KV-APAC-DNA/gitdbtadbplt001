with wks_mi_msl_sales_vs_reco_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_sales_vs_reco_tbl') }}
),
transformed as(    
    SELECT m1.mth_mm, m1.customer_code, m1.customer_name, m1.retailer_code, m1.retailer_name, m1.rtruniquecode, m1.region_name, m1.zone_name, m1.territory_name,
        m1.channel_name, m1.retailer_category_name, m1.salesman_code, m1.salesman_name, m1.unique_sales_code, m1.latest_customer_code, m1.latest_customer_name,m1.latest_territory_code, m1.latest_territory_name, m1.latest_salesman_code, m1.latest_salesman_name, m1.latest_uniquesalescode,
        SUM(m123_msl.quantity) AS quantity,
        SUM(m123_msl.achievement_nr) AS achievement_nr,
        MAX(m1.total_subd) AS total_subd,
        m1.qtr,
        m1.period,
        MAX(m1.msl_count) AS msl_count,
        COUNT(DISTINCT m123_msl.mothersku_name_sold) AS msl_sold_count
    FROM wks_mi_msl_sales_vs_reco_tbl m1
    /*INNER JOIN in_wks.wks_mi_msl_sales_vs_reco_tbl m123
            ON LEFT(m1.mth_mm,4) = LEFT(m123.mth_mm,4)
        AND m1.qtr = m123.qtr
        AND m123.mth_mm <= m1.mth_mm */
    LEFT JOIN wks_mi_msl_sales_vs_reco_tbl m123_msl
            ON LEFT(m1.mth_mm,4) = LEFT(m123_msl.mth_mm,4)
        AND m1.qtr = m123_msl.qtr
        AND m123_msl.mth_mm <= m1.mth_mm
        AND m1.customer_code = m123_msl.customer_code
        AND m1.retailer_code = m123_msl.retailer_code
        AND m1.region_name = m123_msl.region_name
        AND m1.zone_name = m123_msl.zone_name
        AND m1.channel_name = m123_msl.channel_name
        AND m1.ms_flag = m123_msl.ms_flag
    WHERE m1.ms_flag = '1'
    AND m1.achievement_nr > 0
    GROUP BY m1.mth_mm, m1.customer_code, m1.customer_name, m1.retailer_code, m1.retailer_name, m1.rtruniquecode, m1.region_name, m1.zone_name, 
            m1.territory_name, m1.channel_name, m1.retailer_category_name, m1.salesman_code, m1.salesman_name, m1.unique_sales_code, m1.latest_customer_code,
            m1.latest_customer_name, m1.latest_territory_code, m1.latest_territory_name, m1.latest_salesman_code, m1.latest_salesman_name, 
            m1.latest_uniquesalescode,m1.qtr, m1.period
)
select * from transformed