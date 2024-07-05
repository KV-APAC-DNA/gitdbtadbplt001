with wks_pob_cx_final as 
(
    select * from {{ ref('hcpwks_integration__wks_pob_cx_final') }}
),
final as 
(
    select 
    urc::varchar(100) as urc,
	ventasys_product::varchar(200) as ventasys_product,
	year::number(18,0) as year,
	quarter::number(18,0) as quarter,
	month::number(18,0) as month,
	week::number(18,0) as week,
	date::varchar(16) as date,
	pob_units::number(38,2) as pob_units,
	sales_ach_nr::number(38,6) as sales_ach_nr,
	sales_qty::number(38,0) as sales_qty,
	urc_name::varchar(150) as urc_name,
	region_sales::varchar(50) as region_sales,
	territory_sales::varchar(50) as territory_sales,
	zone_sales::varchar(50) as zone_sales,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(150) as distributor_name
    from wks_pob_cx_final
)
select * from final