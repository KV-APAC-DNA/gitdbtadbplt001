with v_rpt_sales_details as 
(
    select * from snapindedw_integration.v_rpt_sales_details
    --{{ ref('indedw_integration__v_rpt_sales_details') }} 
),
wks_pilot_cy_actual_nup as
(
    select * from {{ ref('indwks_integration__wks_pilot_cy_actual_nup') }}
),
itg_query_parameters as
(
    select * from snapinditg_integration.itg_query_parameters
),
final as
(
    SELECT sf.mth_mm,
       sf.qtr as qtr,
       sf.fisc_yr as fisc_yr,
       sf.month as month,
       sf.latest_customer_code AS customer_code,
       sf.latest_customer_name AS customer_name,
       sf.retailer_code as retailer_code,
       sf.rtruniquecode as rtruniquecode,
       sf.mothersku_code as mothersku_code,
       sf.latest_salesman_code as latest_salesman_code,
       sf.latest_salesman_name as latest_salesman_name,
       sf.latest_uniquesalescode as latest_uniquesalescode,
       sf.region_name as region_name,
       sf.zone_name as zone_name,
       sf.latest_territory_name AS territory_name,
       sf.class_desc as class_desc,
       CASE WHEN sf.class_desc IN ('A','SA') THEN 1 ELSE 0 END AS class_desc_a_sa_flag,
       sf.channel_name as channel_name,
       sf.business_channel as business_channel,
       sf.franchise_name as franchise_name,
       sf.brand_name as brand_name,
       sf.product_category_name as product_category_name,
       sf.variant_name as variant_name,
       trim(sf.retailer_name) as retailer_name,
       sf.retailer_category_name as retailer_category_name,
       sf.mothersku_name as mothersku_name,
       sf.status_desc as status_desc,
       nup_act_cy.nup AS nup_actual_cy,
       SUM(sf.quantity) AS quantity,
       SUM(sf.achievement_nr) AS achievement_nr,
       COUNT(DISTINCT sf.num_lines) AS num_lines,
       COUNT(DISTINCT sf.product_code) AS num_packs
    FROM v_rpt_sales_details sf
    LEFT JOIN wks_pilot_cy_actual_nup nup_act_cy
        ON LEFT(sf.mth_mm,4) = LEFT(nup_act_cy.mth_mm,4)
        AND RIGHT(sf.mth_mm,2) = RIGHT(nup_act_cy.mth_mm,2)
        AND sf.rtruniquecode = nup_act_cy.rtruniquecode
    WHERE sf.mth_mm >= (SELECT PARAMETER_VALUE AS PARAMETER_VALUE
                        FROM itg_query_parameters
                        WHERE UPPER(COUNTRY_CODE) = 'IN'
                        AND   UPPER(PARAMETER_TYPE) = 'START_PERIOD'
                        AND   UPPER(PARAMETER_NAME) = 'SKU_RECOM_2024_GT_SSS_START_PERIOD')
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
)
select * from final