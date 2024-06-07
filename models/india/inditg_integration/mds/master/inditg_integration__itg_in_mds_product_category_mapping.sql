with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_product_category_mapping') }}
),
final as 
(
    select
       code::number(18,0) as id,
       'FRANCHISE_NAME_CODE'::varchar(200) as franchise_name,
       'BRAND_NAME_CODE'::varchar(200) as brand_name,
       'PRODUCT_CATEGORY_CODE'::varchar(200) as product_category,
       'VARIANT_NAME_CODE'::varchar(200) as variant_name,
       'PRODUCT_CATEGORY1_CODE'::varchar(200) as product_category1,
       'PRODUCT_CATEGORY2_CODE'::varchar(200) as product_category2,
       'PRODUCT_CATEGORY3_CODE'::varchar(200) as product_category3,
       'PRODUCT_CATEGORY4_CODE'::varchar(200) as product_category4,
       convert_timezone('UTC', current_timestamp()) as crt_dtm,
       enterdatetime::timestamp_ntz(9) as enterdatetime,
       lastchgdatetime::timestamp_ntz(9) as last_change_datetime
    from source
)
select * from final