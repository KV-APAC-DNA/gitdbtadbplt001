with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_sfmc_gender') }}
),
final as 
(   
    select 
    gender_raw::varchar(100) as gender_raw,
	gender_standard_name::varchar(100) as gender_standard,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final