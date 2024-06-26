with 
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
rx_cx_sales_achnr_qty_recom as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_sales_achnr_qty_recom') }}
),
rx_cx_missing_urc_prod_list as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_missing_urc_prod_list') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
final as 
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
FROM  rx_cx_missing_urc_prod_list tmp
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
)
select * from final