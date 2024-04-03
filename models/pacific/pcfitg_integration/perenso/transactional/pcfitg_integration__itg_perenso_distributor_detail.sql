with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_distributor_detail') }}
),
final as
(
    select
        dist_key::number(10,0) as dist_key,
        distributor::varchar(255) as distributor,
        dist_id::varchar(255) as dist_id,
        branch_key::number(10,0) as branch_key,
        display_name::varchar(255) as display_name,
        short_name::varchar(255) as short_name,
        active::varchar(10) as active,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final