with source as (
    select * from {{ source('vnmsdl_raw', 'sdl_vn_product_mapping') }}
)

SELECT
    putupid,
    barcode,
    trim(productname) as productname,
    convert_timezone('Asia/Singapore',current_timestamp) as crt_dttm
FROM source