with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_pos_promotional_price_ean_map') }}
),
src as(
    select distinct customer,cust_prod_cd,barcode from source
),
transformed as(
    select 
       rtrim(REGEXP_REPLACE(src.customer,'[\xC2\xA0]', '')) as customer,
       null as customer_hierarchy_code,
       rtrim(REGEXP_REPLACE(src.cust_prod_cd,'[\xC2\xA0]', '')) as cust_prod_cd,
       rtrim(REGEXP_REPLACE(src.barcode,'[\xC2\xA0]', '')) as barcode,
       null as sap_product_code,
       null as upd_dttm
    from src
)
select * from transformed