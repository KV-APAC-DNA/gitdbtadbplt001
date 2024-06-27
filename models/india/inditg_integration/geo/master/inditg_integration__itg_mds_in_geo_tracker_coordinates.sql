with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_geo_tracker_coordinates') }}
),
final as 
(
    select 
	zone_name_name::varchar(200) as zone_name,
	territory_name_name::varchar(200) as territory_name,
	code::varchar(100) as customer_code,
	name::varchar(200) as customer_name,
	lat_zone_name::varchar(200) as lat_zone,
	null::varchar(200) as long_zone,
	lat_territory::varchar(200) as lat_territory,
	long_territory::varchar(200) as long_territory,
	lat_customer::varchar(200) as lat_customer,
	long_customer::varchar(200) as long_customer,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dt
    from source
)
select * from final