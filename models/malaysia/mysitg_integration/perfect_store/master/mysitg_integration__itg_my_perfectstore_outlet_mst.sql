with source as 
(
    select * from {{ source('myssdl_raw', 'sdl_my_perfectstore_outlet_mst') }}
),
final as
(   
    select
        outlet_no::varchar(255) as outlet_no,
        name::varchar(255) as name,
        zone_no::varchar(255) as zone_no,
        chain_no::varchar(255) as chain_no,
        channel_no::varchar(255) as channel_no,
        address::varchar(255) as address,
        postcode::varchar(255) as postcode,
        latitude::varchar(255) as latitude,
        longitude::varchar(255) as longitude,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        current_timestamp::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final