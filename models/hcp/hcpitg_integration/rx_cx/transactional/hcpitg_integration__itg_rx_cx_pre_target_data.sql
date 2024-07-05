with rx_cx_ret_dim as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_ret_dim') }}
),
final as 
(
    select 
    urc::number(30,0) as urc,
	rx_product::varchar(200) as rx_product,
	rx_units::number(38,2) as rx_units,
	year::number(18,0) as year,
	quarter::number(18,0) as quarter,
	presc_mnth_cnt::number(38,0) as presc_mnth_cnt,
	ach_nr::number(38,6) as ach_nr,
	qty::number(38,0) as qty,
	sales_actv_mnth_cnt::number(38,0) as sales_actv_mnth_cnt,
	urc_name::varchar(150) as urc_name,
	region_sales::varchar(50) as region_sales,
	territory_sales::varchar(50) as territory_sales,
	zone_sales::varchar(50) as zone_sales,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(150) as distributor_name,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from rx_cx_ret_dim
)
select * from final