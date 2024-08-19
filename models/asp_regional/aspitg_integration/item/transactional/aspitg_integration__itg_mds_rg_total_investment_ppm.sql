with sdl_mds_rg_total_investment_ppm as
(
    select * from {{source('aspsdl_raw', 'sdl_mds_rg_total_investment_ppm')}}
),
final as
(
    select
    map.name::varchar(500) as name,
	map.code::varchar(500) as code,
	map.brand::varchar(200) as brand,
	map.product_minor::varchar(200) as product_minor,
	map.mrc_code::varchar(200) as mrc_code,
	map.mrc_name::varchar(200) as mrc_name,
	map.ppm_role::varchar(200) as ppm_role,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as load_date
    from sdl_mds_rg_total_investment_ppm map
)
select * from final