with source as(
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_pos_product_mapping') }}
),
final as(
    select 
        name::varchar(500) as name,
        jnj_sku_code::varchar(200) as jnj_sku_code,
        age_group_name::varchar(500) as age_group_name,
        category_name::varchar(500) as category_name,
        prod_code::varchar(200) as prod_code,
        customer_name::varchar(500) as customer_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    FROM source
)
select * from final