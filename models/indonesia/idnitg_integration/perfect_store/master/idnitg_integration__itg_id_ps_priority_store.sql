with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ps_priority_store') }}
),
final as 
(
    select 
    trim(jjid)::varchar(200) as jjid,
	trim(name)::varchar(500) as store_name,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final