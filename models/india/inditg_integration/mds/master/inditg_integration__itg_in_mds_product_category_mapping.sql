with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_product_category_mapping') }}
),
final as 
(
    select
       code::number(18,0) as id,
       franchise_name_code::varchar(200) as franchise_name,
       brand_name_code::varchar(200) as brand_name,
       product_category_code::varchar(200) as product_category,
       variant_name_code::varchar(200) as variant_name,
       product_category1_code::varchar(200) as product_category1,
       product_category2_code::varchar(200) as product_category2,
       product_category3_code::varchar(200) as product_category3,
       product_category4_code::varchar(200) as product_category4,
       current_timestamp()::timestamp_ntz(9) as crt_dtm,
       enterdatetime::timestamp_ntz(9) as enterdatetime,
       lastchgdatetime::timestamp_ntz(9) as last_change_datetime
    from source
)
select * from final