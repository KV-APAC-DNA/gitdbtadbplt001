with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_ranging_acct_grp_rel') }}
),
final as
(
    select
        ranging_key::number(10,0) as ranging_key,
        acct_grp_key::number(10,0) as acct_grp_key,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final