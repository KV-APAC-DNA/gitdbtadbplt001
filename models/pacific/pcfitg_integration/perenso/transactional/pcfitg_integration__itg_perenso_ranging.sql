with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_ranging') }}
),
final as
(
    select
        ranging_key::number(10,0) as ranging_key,
        ranging_desc::varchar(255) as ranging_desc,
        acct_grp_lev_key::number(10,0) as acct_grp_lev_key,
        active::varchar(5) as active,
        all_accounts::varchar(5) as all_accounts,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp::timestamp_ntz(9) as update_dt
    from source
)
select * from final