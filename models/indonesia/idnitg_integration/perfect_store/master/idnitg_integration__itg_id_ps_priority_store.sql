with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ps_priority_store') }}
),
final as 
(
    select 
    jjid::varchar(200) as jjid,
	store_name::varchar(500) as store_name,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final