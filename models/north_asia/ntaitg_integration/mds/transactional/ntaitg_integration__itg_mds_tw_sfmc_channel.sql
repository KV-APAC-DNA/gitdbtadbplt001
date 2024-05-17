with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_sfmc_channel') }}
),
final as 
(   
    select 
    channel_raw::varchar(200) as channel_raw,
	channel_standard_name::varchar(200) as channel_standard,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final