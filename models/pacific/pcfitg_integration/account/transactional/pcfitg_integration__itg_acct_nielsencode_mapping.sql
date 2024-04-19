with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_acct_nielsencode_mapping') }}
),
final as 
(
    select 
        ac_shortname::varchar(255) as ac_shortname,
        ac_longname::varchar(255) as ac_longname,
        ac_code::varchar(100) as ac_code,
        ac_nielsencode::varchar(100) as ac_nielsencode,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final