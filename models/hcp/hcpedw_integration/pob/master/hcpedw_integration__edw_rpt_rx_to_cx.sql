with 
rx_cx_rpt_tbl as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_rpt_tbl') }}
),
final as 
(
    select
    urc::varchar(50) as urc,
	lysq_ach_nr::number(10,2) as lysq_ach_nr,
	lysq_qty::number(18,0) as lysq_qty,
	lysq_presc::number(10,2) as lysq_presc,
	quarter::varchar(2) as quarter,
	target_presc::number(10,2) as target_presc,
	target_qty::number(18,0) as target_qty,
	"case"::number(18,0) as case,
	prescription_action::varchar(100) as prescription_action,
	sales_action::varchar(100) as sales_action,
	product_vent::varchar(25) as product_vent,
	year::number(18,0) as year,
	hcp::varchar(2000) as hcp,
	prescriptions_needed::number(10,2) as prescriptions_needed,
	hcp_name::varchar(500) as hcp_name,
	emp_name::varchar(500) as emp_name,
	emp_id::varchar(500) as emp_id,
	region_vent::varchar(500) as region_vent,
	territory_vent::varchar(500) as territory_vent,
	zone_vent::varchar(500) as zone_vent,
	actual_ach_nr::number(38,6) as actual_ach_nr,
	actual_qty::number(38,0) as actual_qty,
	actual_presc::number(38,6) as actual_presc,
	urc_name::varchar(150) as urc_name,
	region_sales::varchar(50) as region_sales,
	territory_sales::varchar(50) as territory_sales,
	zone_sales::varchar(50) as zone_sales,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(150) as distributor_name,
	gtm_flag::varchar(150) as gtm_flag,
	product_name_sales::varchar(65535) as product_name_sales,
	franchise_code::varchar(50) as franchise_code,
	franchise_name::varchar(50) as franchise_name,
	salesman_code_sales::varchar(50) as salesman_code_sales,
	salesman_name_sales::varchar(50) as salesman_name_sales,
	convert_timezone('Singapore','Asia/Kolkata',dateadd(minute,-30,current_timestamp() :: timestamp)) as load_date
    from rx_cx_rpt_tbl
)
select * from final