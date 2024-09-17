with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_custom_list') }}
),
final as
(
    select
        acct_key::number(10,0) as acct_key,
        field_key::number(10,0) as field_key,
        option_desc::varchar(10) as option_desc,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt,
        file_name::varchar(255) as file_name
    from source
)
select * from final