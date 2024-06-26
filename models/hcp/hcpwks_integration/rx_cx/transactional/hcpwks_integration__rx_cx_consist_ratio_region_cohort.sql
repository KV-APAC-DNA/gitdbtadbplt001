with 

itg_rx_cx_pre_target_data as 
(
    select * from {{ ref('hcpitg_integration__itg_rx_cx_pre_target_data') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
temp_a as 
(
    SELECT 
       lysq.urc,
       lysq.rx_product,
       lysq.year,
       lysq.quarter,
       lysq.urc_name,
       lysq.region_sales,
       lysq.territory_sales,
       lysq.zone_sales,
       lysq.ach_nr AS lysq_ach_NR,
       lysq.qty AS lysq_qty,
       lysq.rx_units AS lysq_presc,
       (CASE WHEN lysq.presc_mnth_cnt = 3 AND lysq.sales_actv_mnth_cnt = 3 AND lysq.rx_units IS NOT NULL AND lysq.rx_units <> 0 AND lysq.qty IS NOT NULL AND lysq.qty <> 0  THEN 'CONSISTENT OUTLET'
            ELSE 'INCONSISTENT OUTLET'
       END) AS outlet_consistency_tag,
       NVL(lysq_qty,0)/lysq_presc AS ratio,                 
       (CASE WHEN region_sales = 'EAST' THEN 'REGION1'
             WHEN region_sales IN ('EAST CENTRAL','SOUTH','SOUTH CENTRAL','WEST') THEN 'REGION2'
             WHEN region_sales IN ('NORTH','NORTH CENTRAL') THEN 'REGION3'
        END) AS region_cohort                               
FROM  itg_rx_cx_pre_target_data lysq
WHERE lysq.year = (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                   FROM   itg_query_parameters
                   WHERE  UPPER(country_code) = 'IN'
                   AND    UPPER(parameter_type) = 'YEAR'
                   AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_YEAR') - 1
  AND lysq.quarter = (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                      FROM   itg_query_parameters
                      WHERE  UPPER(country_code) = 'IN'
                      AND    UPPER(parameter_type) = 'QUARTER'
                      AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_QTR')
)
-- ,temp_b as 
-- (
--     SELECT 
--        urc,
--        rx_product,
--        year,
--        quarter,
--        NULL AS urc_name,
--        region_sales,
--        NULL AS territory_sales,
--        NULL AS zone_sales,
--        ach_NR AS lysq_ach_nr,
--        qty AS lysq_qty,
--        rx_units AS lysq_presc,
--        NULL AS outlet_consistency_tag,
--        NULL AS ratio,
--        region_cohort
-- FROM  rx_cx_miss_urc_prod_sales_rx
-- ),
-- final as 
-- (
--     select * from temp_a
--     union all
--     select * from temp_b
-- )
select * from temp_a