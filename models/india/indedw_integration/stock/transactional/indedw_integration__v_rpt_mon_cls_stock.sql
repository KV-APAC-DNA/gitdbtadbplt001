with edw_mth_cls_stock_fact as
(
    select * from snapindedw_integration.edw_mth_cls_stock_fact
),
edw_retailer_calendar_dim as
(
    select * from snapindedw_integration.edw_retailer_calendar_dim
),
edw_customer_dim as
(
    select * from snapindedw_integration.edw_customer_dim
),
edw_product_dim as
(
    select * from snapindedw_integration.edw_product_dim
),
edw_user_dim as
(
    select * from snapindedw_integration.edw_user_dim
),
final as
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
    cal.fisc_yr AS "year", 
    cal.qtr, 
    cal.month_nm_shrt AS "month", 
    stock.calsalclosing, 
    stock.calsalclosingval, 
    stock.calunsalclosing, 
    stock.calunsalclosingval, 
    stock.calofferclosing, 
    stock.calofferclosingval, 
    stock.salstockadjqty, 
    stock.salstockadjval, 
    stock.unsalstockadjqty, 
    stock.unsalstockadjval, 
    stock.offerstockadjqty, 
    stock.offerstockadjval, 
    cust.num_of_retailers AS retailer_count, 
    cust.type_name, 
    CASE WHEN (ud.abi_ntid IS NULL) THEN 'Unknown' :: character varying ELSE ud.abi_ntid END AS abi_ntid, 
    CASE WHEN (ud.flm_ntid IS NULL) THEN 'Unknown' :: character varying ELSE ud.flm_ntid END AS flm_ntid, 
    CASE WHEN (ud.bdm_ntid IS NULL) THEN 'Unknown' :: character varying ELSE ud.bdm_ntid END AS bdm_ntid, 
    CASE WHEN (ud.rsm_ntid IS NULL) THEN 'Unknown' :: character varying ELSE ud.rsm_ntid END AS rsm_ntid 
    FROM ((((edw_mth_cls_stock_fact stock 
            LEFT JOIN (
                SELECT 
                DISTINCT edw_retailer_calendar_dim.mth_yyyymm, 
                edw_retailer_calendar_dim.qtr, 
                edw_retailer_calendar_dim.fisc_yr, 
                edw_retailer_calendar_dim.month_nm_shrt 
                FROM 
                edw_retailer_calendar_dim
            ) cal 
            ON ((((stock.stock_month):: text = ((cal.mth_yyyymm):: character varying):: text) 
            AND ((stock.stock_year):: text = ((cal.fisc_yr):: character varying):: text)
                ))) 
            LEFT JOIN edw_customer_dim cust 
            ON (((stock.customer_code):: text = (cust.customer_code):: text)
            )) 
        LEFT JOIN edw_product_dim prod 
        ON (((stock.product_code):: text = (prod.product_code):: text)
        )) 
        LEFT JOIN edw_user_dim ud 
        ON (((((cust.region_name):: text = (ud.region_name):: text) 
        AND ((cust.zone_name):: text = (ud.zone_name):: text)) 
        AND ((cust.territory_name):: text = (ud.territory_name):: text)))
    )
)
select * from final