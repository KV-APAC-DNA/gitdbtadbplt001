with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_acct_dist_acct') }}
),
final as
(
    select
        acct_key::number(10,0) as acct_key,
        branch_key::number(10,0) as branch_key,
        id::varchar(10) as id,
        system_primary::varchar(10) as system_primary,
        active::varchar(10) as active,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final