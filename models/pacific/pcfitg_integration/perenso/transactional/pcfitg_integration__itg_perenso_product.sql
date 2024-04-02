with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_product') }}
),
final as(
    select
        prod_key::number(10,0) as prod_key,
        prod_id::varchar(50) as prod_id,
        prod_desc::varchar(100) as prod_desc,
        prod_ean::varchar(50) as prod_ean,
        active::varchar(10) as active,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final
