with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_pos_promotional_price_ean_map') }}
),
transformed as(
    select source.customer as customer,
       RTRIM(source.barcode,'') as barcode,
       RTRIM(source.cust_prod_cd,'') as cust_prod_cd,
       source.promotional_price as promotional_price,
       source.promotion_start_date as promotion_start_date,
       source.promotion_end_date as promotion_end_date,
       null as updt_dttm
    from source
)
select * from transformed
