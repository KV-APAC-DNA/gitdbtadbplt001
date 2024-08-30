with source as (
    select * from {{ source('vnmsdl_raw', 'sdl_vn_product_mapping') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_product_mapping__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_product_mapping__duplicate_test')}}
    )
)

SELECT
    putupid,
    barcode,
    trim(productname) as productname,
    convert_timezone('Asia/Singapore',current_timestamp) as crt_dttm
FROM source