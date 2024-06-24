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
final as 
(
    SELECT 
        ret.rtruniquecode as urc,
        mapp.prod_vent,
        SUM(sales.achievement_nr_val) AS ach_NR,
        SUM(sales.quantity) AS qty,
        cal.fisc_yr,
        cal.qtr,
        cal.mth_yyyymm AS month,
        cal.week AS week,
        sales.invoice_date AS invoice_date
FROM edw_dailysales_fact sales
INNER JOIN edw_retailer_calendar_dim cal 
        ON sales.invoice_date = cal.day
INNER JOIN edw_product_dim prod
        ON sales.product_code = prod.product_code
INNER JOIN (SELECT  SPLIT_PART(parameter_name,'-',1) AS prod_vent, 
                    SPLIT_PART(parameter_value,'-',1) AS product_code
            FROM itg_query_parameters
            WHERE parameter_type = 'Rx_to_Cx_to_Pob_Product_Mapping'
            GROUP BY 1,2) mapp          
        ON mapp.product_code = prod.product_code
       AND UPPER(mapp.prod_vent) LIKE 'ORSL%'
INNER JOIN edw_retailer_dim ret
        ON sales.retailer_code = ret.retailer_code
       AND sales.customer_code = ret.customer_code
       AND ret.actv_flg = 'Y'
WHERE cal.fisc_yr >= EXTRACT(YEAR FROM current_timestamp()) - 2   
  AND cal.qtr >=2            
GROUP BY 1,
         2,
         5,
         6,
         7,
         8,
         9
)
select * from final