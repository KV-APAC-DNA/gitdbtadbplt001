with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_type') }}
),
final as
(
    select
        acct_type_key::number(10,0) as acct_type_key,
        acct_type_desc::varchar(50) as acct_type_desc,
        dsp_order::number(2,0) as dsp_order,
        active::varchar(10) as active,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt
    from source
)
select * from final