with source as(
    select * from {{ source('myssdl_raw', 'sdl_my_ciw_map') }}
),
final as (
    select 
        ciw_ctgry::varchar(100) as ciw_ctgry,
        ciw_buckt1::varchar(100) as ciw_buckt1,
        ciw_buckt2::varchar(100) as ciw_buckt2,
        bravo_cd1::varchar(20) as bravo_cd1,
        bravo_desc1::varchar(100) as bravo_desc1,
        bravo_cd2::varchar(20) as bravo_cd2,
        bravo_desc2::varchar(100) as bravo_desc2,
        acct_type::varchar(20) as acct_type,
        cast(acct_num as int) as acct_num,
        acct_desc::varchar(100) as acct_desc,
        acct_type1::varchar(20) as acct_type1,
        cdl_dttm::varchar(100) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final