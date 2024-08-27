{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}

with edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_day_cls_stock_fact as
(
    select * from {{ ref('indedw_integration__edw_day_cls_stock_fact') }}
),
edw_customer_dim as
(
    select * from {{ ref('indedw_integration__edw_customer_dim') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
final as
(
    SELECT 
    stk.customer_code, 
    stk.customer_name, 
    stk.region_name, 
    stk.state_name, 
    stk.territory_classification, 
    stk.territory_name, 
    stk.town_name, 
    stk.zone_name, 
    stk.brand_name, 
    stk.franchise_name, 
    stk.mothersku_name, 
    stk.product_category_name, 
    stk.product_name, 
    stk.variant_name, 
    stk.product_code, 
    stk."year", 
    stk.qtr, 
    stk."month", 
    stk.week, 
    stk.stock_date, 
    stk.unsalclsstock, 
    stk.offerclsstock, 
    stk.salclsstock, 
    stk.salclsstockval, 
    stk.salstockin, 
    stk.salstockout, 
    stk.unsalstockin, 
    stk.unsalstockout, 
    stk.offerstockin, 
    stk.offerstockout, 
    stk.nr, 
    stk.num_of_retailers, 
    stk.abi_ntid, 
    stk.flm_ntid, 
    stk.bdm_ntid, 
    stk.rsm_ntid, 
    stk.type_name 
    FROM 
    (
        SELECT 
        stock.customer_code, 
        cust.customer_name, 
        cust.region_name, 
        cust.state_name, 
        cust.territory_classification, 
        cust.territory_name, 
        cust.town_name, 
        cust.zone_name, 
        prod.brand_name, 
        prod.franchise_name, 
        prod.mothersku_name, 
        prod.product_category_name, 
        prod.product_name, 
        prod.variant_name, 
        stock.product_code, 
        cal.cal_yr AS "year", 
        cal.qtr, 
        cal.month_nm_shrt AS "month", 
        cal.week, 
        stock.stock_date, 
        stock.unsalclsstock, 
        stock.offerclsstock, 
        stock.salclsstock, 
        stock.salclsstockval, 
        stock.salstockin, 
        stock.salstockin AS salstockout, 
        stock.unsalstockin, 
        stock.unsalstockout, 
        stock.offerstockin, 
        stock.offerstockout, 
        stock.nr, 
        cust.num_of_retailers, 
        NULL :: character varying AS abi_ntid, 
        NULL :: character varying AS flm_ntid, 
        NULL :: character varying AS bdm_ntid, 
        NULL :: character varying AS rsm_ntid, 
        cust.type_name 
        FROM 
        edw_retailer_calendar_dim cal, 
        ((edw_day_cls_stock_fact stock LEFT JOIN edw_customer_dim cust 
        ON (((stock.customer_code):: text = (cust.customer_code):: text))) 
            LEFT JOIN edw_product_dim prod 
            ON (((stock.product_code):: text = (prod.product_code):: text))) 
        WHERE to_date(stock.stock_date) = to_date(cal.caldate)
    ) stk

)
select * from final