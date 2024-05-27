with source as
(
    select * from {{source('idnsdl_raw','sdl_mds_id_5ps_store_mapping') }}
),
final as
(
    select
    name::varchar(500) as name,
	code:: varchar(500) as store_id,
	jnjid:: varchar(200) as jnjid,
	account:: varchar(200) as account,
	distributor_id:: varchar(200) as distributor_id,
	distributor_name:: varchar(200) as distributor_name,
	region:: varchar(200) as region,
	area:: varchar(200) as area,
	city:: varchar(200) as city,
	address:: varchar(2000) as address,
	tiering:: varchar(200) as tiering,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)

select * from final