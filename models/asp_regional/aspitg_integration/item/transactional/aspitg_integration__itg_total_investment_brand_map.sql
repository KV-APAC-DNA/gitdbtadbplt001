with sdl_mds_rg_total_investment_brand_map as
(
    select * from {{source('aspsdl_raw', 'sdl_mds_rg_total_investment_brand_map')}}
),
final as
(
    select
    map2.name::varchar(500) as name,
	map2.code::varchar(500) as code,
	map2.bravo_product_code::varchar(200) as bravo_product_code,
	map2.brand::varchar(200) as brand,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as load_date
    from sdl_mds_rg_total_investment_brand_map MAP2
)
select * from final