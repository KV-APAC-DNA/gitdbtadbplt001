with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_pos_promotional_price_ean_map') }}
),
src as(
    select distinct customer,cust_prod_cd,barcode from source
),
transformed as(
    select trim(src.customer) as customer,
       null as customer_hierarchy_code,
       trim(src.cust_prod_cd,'') as cust_prod_cd,
       trim(src.barcode,'') as barcode,
       null as sap_product_code,
       null as upd_dttm
    from src
)
select * from transformed