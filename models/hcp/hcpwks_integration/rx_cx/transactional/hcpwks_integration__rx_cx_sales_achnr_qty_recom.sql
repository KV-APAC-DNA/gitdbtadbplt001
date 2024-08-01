with 
edw_dailysales_fact as 
(
    select * from {{ ref('indedw_integration__edw_dailysales_fact') }}
),
edw_retailer_calendar_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_product_dim as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
itg_ventasys_jnj_prod_mapping as 
(
    select * from {{ source('inditg_integration', 'itg_ventasys_jnj_prod_mapping') }}
),
final as 
(
    SELECT 
        ret.rtruniquecode as urc,
        mapp.prod_vent,
        SUM(sales.achievement_nr_val) AS ach_NR,
        SUM(sales.quantity) AS qty,
        cal.fisc_yr,
        cal.qtr
FROM edw_dailysales_fact sales
  INNER JOIN edw_retailer_calendar_dim cal 
          ON sales.invoice_date = cal.day
  INNER JOIN edw_product_dim prod
          ON sales.product_code = prod.product_code
  INNER JOIN itg_ventasys_jnj_prod_mapping mapp
          ON mapp.prod_name = prod.variant_name
         AND (mapp.prod_vent = 'ORSL (Core)'
          OR mapp.prod_vent = 'ORSL (Plus)'
          OR mapp.prod_vent = 'ORSL (Rehydrate)')
  INNER JOIN edw_retailer_dim ret
          ON sales.retailer_code = ret.retailer_code
         AND sales.customer_code = ret.customer_code
         AND ret.actv_flg = 'Y'
WHERE cal.fisc_yr =   (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                       FROM   itg_query_parameters
                       WHERE  UPPER(country_code) = 'IN'
                       AND    UPPER(parameter_type) = 'YEAR'
                       AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_YEAR') - 1
  AND cal.qtr = (SELECT CAST(parameter_value AS INTEGER) AS parameter_value
                 FROM   itg_query_parameters
                 WHERE  UPPER(country_code) = 'IN'
                 AND    UPPER(parameter_type) = 'QUARTER'
                 AND    UPPER(parameter_name) = 'IN_RX_CX_TARGET_GEN_QTR')
GROUP BY 1,
         2,
         5,
         6

)
select * from final