with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_product_mapping') }}
),

final as (
    select
        customer_name_code::varchar(100) as customer_name,
        customer_brand_code::varchar(200) as customer_brand,
        customer_product_code::varchar(300) as customer_product_code,
        customer_product_name::varchar(500) as customer_product_name,
        "master code"::varchar(300) as master_code,
        "material code"::varchar(300) as material_code,
        "product name"::varchar(500) as product_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
