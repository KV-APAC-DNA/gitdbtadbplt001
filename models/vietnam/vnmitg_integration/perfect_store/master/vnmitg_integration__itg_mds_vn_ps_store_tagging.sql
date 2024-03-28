with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_ps_store_tagging') }}
),

final as
(
    select
    parent_customer::varchar(256) as parent_customer,
	store_code::varchar(256) as store_code,
	store_name::varchar(256) as store_name,
	status::varchar(256) as status,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    from source
)
select * from final