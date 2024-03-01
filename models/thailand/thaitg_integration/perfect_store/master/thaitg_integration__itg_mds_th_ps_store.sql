with source as(
    select * from {{ source('thasdl_raw', 'sdl_mds_th_ps_store') }}
),
final as(
    select
        dataset::varchar(200) as dataset,
        channel::varchar(200) as channel,
        retail_environment::varchar(200) as retail_environment,
        state::varchar(200) as state,
        customer_code::varchar(200) as customer_code,
        customer_name::varchar(200) as customer_name,
        store_code::varchar(200) as store_code,
        store_name::varchar(200) as store_name,
        current_timestamp()::timestampntz(9) as crt_dttm
    from source
)
select * from final