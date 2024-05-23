with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_dms_dist_margin_control') }}
),
final as
(
    select
    distributorcode::varchar(10) as distributorcode,
	franchise::varchar(50) as franchise,
	brand::varchar(50) as brand,
	type::varchar(10) as type,
	margin:: varchar(50) as margin,
	effective_from::varchar(6) as effective_from,
	effective_to::varchar(6) as effective_to,
	current_timestamp()::timestamp_ntz as crtd_dttm
    from source
)
select * from final