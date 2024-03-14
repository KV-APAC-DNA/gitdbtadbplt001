with source as
(
    select * from {{ source('thasdl_raw', 'sdl_th_sfmc_consumer_master_additional') }}
),

final as
(
    select
        'TH'::varchar(10) as cntry_cd,
        subscriber_key::varchar(100) as subscriber_key,
        attribute_name::varchar(100) as attribute_name,
        attribute_value::varchar(100) as attribute_value,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
)

select * from final