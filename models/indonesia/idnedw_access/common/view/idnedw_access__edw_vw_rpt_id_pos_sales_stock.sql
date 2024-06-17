with source as
(
    select * from {{ref('idnedw_integration__edw_vw_rpt_id_pos_sales_stock')}}
)

select
    sap_cntry_cd AS "sap_cntry_cd",
    sap_cntry_nm AS "sap_cntry_nm",
    dataset AS "dataset",
    dstrbtr_grp_cd AS "dstrbtr_grp_cd",
    "year" AS "year",
    yearmonth AS "yearmonth",
    customer_brnch_code AS "customer_brnch_code",
    customer_brnch_name AS "customer_brnch_name",
    customer_store_code AS "customer_store_code",
    customer_store_name AS "customer_store_name",
    customer_franchise AS "customer_franchise",
    customer_brand AS "customer_brand",
    customer_product_code AS "customer_product_code",
    customer_product_desc AS "customer_product_desc",
    jj_sap_prod_id AS "jj_sap_prod_id",
    brand AS "brand",
    brand2 AS "brand2",
    sku_sales_cube AS "sku_sales_cube",
    customer_product_range AS "customer_product_range",
    customer_product_group AS "customer_product_group",
    customer_store_class AS "customer_store_class",
    customer_store_channel AS "customer_store_channel",
    sales_qty AS "sales_qty",
    sales_value AS "sales_value",
    service_level AS "service_level",
    sales_order AS "sales_order",
    share AS "share",
    store_stock_qty AS "store_stock_qty",
    store_stock_value AS "store_stock_value",
    branch_stock_qty AS "branch_stock_qty",
    branch_stock_value AS "branch_stock_value",
    stock_uom AS "stock_uom",
    stock_days AS "stock_days",
    crtd_dttm AS "crtd_dttm"
from source