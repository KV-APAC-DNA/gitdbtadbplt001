with sdl_ecommerce_offtake_master_mapping as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_master_mapping') }}
),
final as
(
    select map.load_date,
    map.source_file_name,
    map.account_name,
    map.account_sku_code,
    map.sku_name_in_file,
    map.brand_name,
    map.lakshya_sku_name,
    map.ean from ( select row_number() over(partition by account_name,account_sku_code,ean order by account_sku_code) as rownum,
    load_date,
    source_file_name,
    trim(account_name) as account_name,
    account_sku_code,
    sku_name_in_file,
    brand_name,
    lakshya_sku_name,
    ean
    
    from sdl_ecommerce_offtake_master_mapping ) map where map.rownum = 1
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    account_name::varchar(255) as account_name,
    account_sku_code::varchar(255) as account_sku_code,
    sku_name_in_file::varchar(2000) as sku_name_in_file,
    brand_name::varchar(255) as brand_name,
    lakshya_sku_name::varchar(255) as lakshya_sku_name,
    ean::varchar(255) as ean
   
 from final