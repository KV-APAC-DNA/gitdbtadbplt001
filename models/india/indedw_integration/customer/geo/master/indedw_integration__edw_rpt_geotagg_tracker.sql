with 
wks_geotag_final_integ as 
(
    select * from {{ ref('indwks_integration__wks_geotag_final_integ') }}
),
final as 
(
    select 
    fisc_yr::varchar(200) as fisc_yr,
	mth_mm::varchar(200) as mth_mm,
	retailer_code::varchar(200) as retailer_cd,
	retailer_name::varchar(200) as retailer_name,
	rtruniquecode::varchar(200) as rtruniquecode,
	channel_name::varchar(100) as channel_name,
	retailer_channel_3::varchar(100) as retailer_channel_3,
	loyalty_program_name::varchar(200) as loyalty_program_name,
	qtr_target::number(18,6) as qtr_target,
	qtr_actuals::number(18,6) as qtr_actuals,
	month_target::number(18,6) as month_target,
	month_actuals::number(18,6) as month_actuals,
	os_flag::varchar(200) as os_flag,
	msl_recom::number(18,0) as msl_recom,
	msl_lines_sold_p3m::number(18,0) as msl_lines_sold_p3m,
	region_name::varchar(200) as region_name,
	zone_name::varchar(200) as zone_name,
	territory_name::varchar(200) as territory_name,
	rtrlatitude::varchar(200) as rtrlatitude,
	rtrlongitude::varchar(200) as rtrlongitude,
	latest_customer_code::varchar(200) as latest_customer_code,
	latest_customer_name::varchar(200) as latest_customer_name,
	latest_salesman_code::varchar(200) as latest_salesman_code,
	latest_salesman_name::varchar(200) as latest_salesman_name,
	latest_uniquesalescode::varchar(100) as latest_uniquesalescode,
	latest_region_name::varchar(100) as latest_region_name,
	latest_zone_name::varchar(100) as latest_zone_name,
	latest_territory_name::varchar(100) as latest_territory_name,
	nielsen_popstrata::varchar(100) as nielsen_popstrata,
	nielsen_statename::varchar(100) as nielsen_statename,
	lat_zone::varchar(200) as lat_zone,
	long_zone::varchar(200) as long_zone,
	lat_territory::varchar(200) as lat_territory,
	long_territory::varchar(200) as long_territory,
	lat_customer::varchar(200) as lat_customer,
	long_customer::varchar(200) as long_customer,
	'SALES_CUBE_BASE'::varchar(50) as dataset,
    from wks_geotag_final_integ
)
select * from final