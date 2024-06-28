with 

itg_rx_cx_pre_target_data as 
(
    select * from {{ ref('hcpitg_integration__itg_rx_cx_pre_target_data') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from snapinditg_integration.itg_hcp360_in_ventasys_rtlmaster
), --hcp

edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
rx_cx_sales_achnr_qty_recom as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_sales_achnr_qty_recom') }}
),
union_1 as 
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
),
prod_list as 
(
    SELECT inn.urc,
       inn.rx_product
FROM (SELECT DISTINCT urc, 'ORSL (Core)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp1
      WHERE tmp1.urc IS NOT NULL
      UNION 
      SELECT DISTINCT urc, 'ORSL (Plus)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp2
      WHERE tmp2.urc IS NOT NULL
      UNION 
      SELECT DISTINCT urc, 'ORSL (Rehydrate)' AS rx_product
      FROM itg_hcp360_in_ventasys_rtlmaster tmp3
      WHERE tmp3.urc IS NOT NULL
      ) inn
EXCEPT
SELECT tmp.urc, tmp.rx_product
FROM  union_1 tmp
GROUP BY 1,2
),
rx_list as
(
    SELECT 
       tmp.urc,
       tmp.rx_product,
       ret.region_sales,
       (CASE WHEN ret.region_sales = 'EAST' THEN 'REGION1'
             WHEN ret.region_sales IN ('EAST CENTRAL','SOUTH','SOUTH CENTRAL','WEST') THEN 'REGION2'
             WHEN ret.region_sales IN ('NORTH','NORTH CENTRAL') THEN 'REGION3'
        END) AS region_cohort,
       (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
        FROM   itg_query_parameters
        WHERE  UPPER(country_code) = 'IN'
        AND    UPPER(parameter_type) = 'YEAR'
        AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_YEAR') - 1 AS year,
       (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
        FROM   itg_query_parameters
        WHERE  UPPER(country_code) = 'IN'
        AND    UPPER(parameter_type) = 'QUARTER'
        AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_QTR') AS quarter,                        
       NVL(sales.ach_NR,0) AS ach_NR,
       NVL(sales.qty,0) AS qty,
       0 AS rx_units
FROM  prod_list tmp
LEFT JOIN  (SELECT urc,
                   prod_vent,
                   fisc_yr,
                   qtr,
                   SUM(ach_NR) AS ach_NR,
                   SUM(qty) AS qty
            FROM rx_cx_sales_achnr_qty_recom sales
            GROUP BY 1,2,3,4) sales
      ON tmp.urc::text = sales.urc
     AND tmp.rx_product = sales.prod_vent
LEFT JOIN (SELECT region_name AS region_sales,
                  rtruniquecode AS urc,
                  ROW_NUMBER() OVER (PARTITION BY rtruniquecode ORDER BY start_date DESC) AS rnk
           FROM edw_retailer_dim
           WHERE actv_flg = 'Y') ret
        ON tmp.urc::text = ret.urc
        AND ret.rnk = 1
),
union_2 as 
(
     SELECT 
       urc,
       rx_product,
       year,
       quarter,
       NULL AS urc_name,
       region_sales,
       NULL AS territory_sales,
       NULL AS zone_sales,
       ach_NR AS lysq_ach_nr,
       qty AS lysq_qty,
       rx_units AS lysq_presc,
       NULL AS outlet_consistency_tag,
       NULL AS ratio,
       region_cohort
FROM rx_list
),
final as 
(
    select * from union_1 
    union all
    select * from union_2
)
select * from final