with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_store_master') }}
),

final as (
    select
        customer_name::varchar(200) as customer_name,
        customer_store_code::varchar(200) as customer_store_code,
        customer_store_name::varchar(200) as customer_store_name,
        customer_store_location::varchar(200) as customer_store_location,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final
