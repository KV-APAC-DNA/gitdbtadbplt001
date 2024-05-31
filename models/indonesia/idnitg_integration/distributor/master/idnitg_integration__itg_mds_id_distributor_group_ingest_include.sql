with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_distributor_group_ingest_include') }}
),
final as
(
    select 
    trim(name)::varchar(30) as default_months,
	trim(code)::varchar(30) as adhoc_months,
	upper(trim(enable_adhoc_load))::varchar(30) as enable_adhoc_load,
	upper(trim(source))::varchar(30) as source,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final