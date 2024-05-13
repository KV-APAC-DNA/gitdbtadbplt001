with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_sfmc_member_tier') }}
),
final as 
(   
    select 
    tier_raw::varchar(100) as tier_raw,
	tier_standard_name::varchar(100) as tier_standard,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final