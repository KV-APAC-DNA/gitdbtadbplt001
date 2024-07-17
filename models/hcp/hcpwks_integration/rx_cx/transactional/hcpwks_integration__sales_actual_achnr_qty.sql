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
itg_ventasys_jnj_prod_mapping as
(
    select * from {{ source('inditg_integration', 'itg_ventasys_jnj_prod_mapping') }}
),
itg_query_parameters as 
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
trans as 
(
    SELECT cal.cal_yr,
        cal.qtr,
        mapp.prod_vent,
        ret.rtruniquecode as urc,
        sum(sales.achievement_nr_val) as actual_ach_nr,
        sum(sales.quantity) as actual_qty
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
WHERE cal.fisc_yr > EXTRACT(YEAR FROM current_timestamp()) - (SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
                                                   FROM   itg_query_parameters
                                                   WHERE  UPPER(COUNTRY_CODE) = 'IN'
                                                     AND  UPPER(PARAMETER_TYPE) = 'YEAR_RANGE'
                                                     AND  UPPER(PARAMETER_NAME) = 'IN_RX_CX_DATA_RETENSION_PERIOD') 
GROUP BY 1,
         2,
         3,
         4
),
final as 
(
    select 
	cal_yr::number(18,0) as cal_yr,
	qtr::number(18,0) as qtr,
	prod_vent::varchar(200) as prod_vent,
	urc::varchar(100) as urc,
	actual_ach_nr::number(38,6) as actual_ach_nr,
	actual_qty::number(38,0) as actual_qty
    from trans
)
select * from final