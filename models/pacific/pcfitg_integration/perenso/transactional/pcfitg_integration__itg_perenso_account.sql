with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account') }}
),
final as
(
    select
        acct_key::number(10,0) as acct_key,
        disp_name::varchar(256) as disp_name,
        acct_type_key::number(10,0) as acct_type_key,
        active::varchar(10) as active,
        acct_street1::varchar(256) as acct_street1,
        acct_street2::varchar(256) as acct_street2,
        acct_street3::varchar(256) as acct_street3,
        acct_suburb::varchar(25) as acct_suburb,
        acct_postcode::varchar(25) as acct_postcode,
        acct_statecode::varchar(20) as acct_statecode,
        acct_state::varchar(20) as acct_state,
        acct_country::varchar(20) as acct_country,
        acct_phone::varchar(50) as acct_phone,
        acct_fax::varchar(50) as acct_fax,
        acct_email::varchar(256) as acct_email,
        run_id::number(14,0) as run_id,
        create_dt::timestamp_ntz(9) as create_dt,
        current_timestamp()::timestamp_ntz(9) as update_dt
    from source
)
select * from final