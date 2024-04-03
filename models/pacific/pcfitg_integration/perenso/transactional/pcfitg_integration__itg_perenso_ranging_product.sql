with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_ranging_product') }}
),
final as
(
    select
        ranging_key::number(10,0) as ranging_key,
        prod_key::number(10,0) as prod_key,
        acct_grp_key::number(10,0) as acct_grp_key,
        core::varchar(5) as core,
        range_rank::varchar(500) as range_rank,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final