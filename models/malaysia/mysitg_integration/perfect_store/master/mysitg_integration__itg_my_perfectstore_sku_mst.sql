with source as (
     select * from {{ source('myssdl_raw','sdl_my_perfectstore_sku_mst') }} 
),
final as (
    select
        sku_no::varchar(255) as sku_no,
        description::varchar(255) as description,
        client::varchar(255) as client,
        manufacture::varchar(255) as manufacture,
        category::varchar(255) as category,
        brand::varchar(255) as brand,
        sub_catgory::varchar(255) as sub_catgory,
        sub_brand::varchar(255) as sub_brand,
        packsize::varchar(255) as packsize,
        other::varchar(255) as other,
        product_barcode::varchar(255) as product_barcode,
        list_price_fib::varchar(255) as list_price_fib,
        list_price_unit::varchar(255) as list_price_unit,
        rcp::varchar(255) as rcp,
        packing_config::varchar(255) as packing_config,
        run_id::numeric(14) as run_id,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from final
)
select * from final