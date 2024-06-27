with 
wks_rx_to_cx_to_pob as 
(
    select * from {{ ref('hcpwks_integration__wks_rx_to_cx_to_pob') }}
),
final as 
(
    select 
	"year"::varchar(10) as year,
	"month"::varchar(10) as month,
	urc::varchar(100) as urc,
	ventasys_product::varchar(200) as ventasys_product,
    franchise_name::varchar(100) as franchise_name,
    brand_name::varchar(100) as brand_name,
	sales_qty::number(38,0) as sales_qty,
	sales_ach_nr::number(38,6) as sales_ach_nr,
	pob_units::number(38,2) as pob_units,
	hcp_id::varchar(50) as hcp_id,
	hcp_name::varchar(100) as hcp_name,
	rx_factorized::number(38,2) as rx_factorized,
	rx_units::number(38,2) as rx_units,
	urc_name::varchar(100) as urc_name,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(100) as distributor_name,
    channel_name::varchar(100) as channel_name,
    class_desc::varchar(100) as class_desc,
    retailer_category_name::varchar(100) as retailer_category_name,
    retailer_channel_1::varchar(100) as retailer_channel_1,
    retailer_channel_2::varchar(100) as retailer_channel_2,
    retailer_channel_3::varchar(100) as retailer_channel_3,
	region_name::varchar(100) as region_name,
	zone_name::varchar(100) as zone_name,
	territory_name::varchar(100) as territory_name,
	salesman_code_sales::varchar(50) as salesman_code_sales,
	salesman_name_sales::varchar(100) as salesman_name_sales,
	emp_id::varchar(50) as emp_id,
	emp_name::varchar(100) as emp_name,
	region_vent::varchar(100) as region_vent,
	territory_vent::varchar(100) as territory_vent,
	zone_vent::varchar(100) as zone_vent,
	"URC Active Flag"::varchar(1) as "urc active flag",
    "URC Active Flag Ventasys"::varchar(1) as "urc active flag ventasys",
	current_timestamp()::timestamp_ntz(9) as load_dttm
    from wks_rx_to_cx_to_pob
)
select * from final