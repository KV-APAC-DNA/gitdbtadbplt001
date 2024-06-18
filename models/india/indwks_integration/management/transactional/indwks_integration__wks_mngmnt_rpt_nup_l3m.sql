with wks_mngmnt_rpt_nup as 
(
    select * from {{ ref('indwks_integration__wks_mngmnt_rpt_nup') }}
),
base_tbl as
(
SELECT mth_mm, TO_DATE(mth_mm::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) AS base_date
FROM wks_mngmnt_rpt_nup
GROUP BY 1,2
),
final as 
(
     SELECT base.mth_mm, intrm.region_name, intrm.zone_name, intrm.territory_name, intrm.customer_code, intrm.retailer_code, intrm.channel_name,
        intrm.franchise_name, intrm.brand_name, intrm.product_category_name, intrm.variant_name, intrm.mothersku_name, intrm.ret_msku,
        intrm.ms_flag, SUM(intrm.quantity) AS quantity, SUM(intrm.achievement_nr) AS achievement_nr
    FROM base_tbl base
    LEFT JOIN wks_mngmnt_rpt_nup intrm
            ON DATEDIFF (month,intrm.sales_date::DATE,base.base_date::DATE) < 3
        AND DATEDIFF (month,intrm.sales_date::DATE,base.base_date::DATE) >= 0
    GROUP BY  base.mth_mm, intrm.region_name, intrm.zone_name, intrm.territory_name, intrm.customer_code, intrm.retailer_code, intrm.channel_name,
            intrm.franchise_name, intrm.brand_name, intrm.product_category_name, intrm.variant_name, intrm.mothersku_name, intrm.ret_msku,
            intrm.ms_flag
)
select * from final