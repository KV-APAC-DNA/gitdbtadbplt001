with 
source as 
(
    select * from {{ source('ntasdl_raw', 'sdl_tw_sales_target') }}
),
final as
(
    select
	fiscal_year_period::varchar(12) as fiscal_year_period,
	j_j_fiscal_week::varchar(50) as j_j_fiscal_week,
	company_code::varchar(4) as company_code,
	value_type::varchar(50) as value_type,
	version::number(18,0) as version,
	manual_type::varchar(10) as manual_type,
	currency::varchar(5) as currency,
	sales_organization::varchar(4) as sales_organization,
	distribution_channel::varchar(2) as distribution_channel,
	division::varchar(5) as division,
	material::varchar(10) as material,
	b1_mega_brand::varchar(50) as b1_mega_brand,
	b2_brand::varchar(50) as b2_brand,
	b3_base_product::varchar(50) as b3_base_product,
	b4_variant::varchar(50) as b4_variant,
	b5_put_up::varchar(50) as b5_put_up,
	operating_group::varchar(50) as operating_group,
	franchise_group::varchar(50) as franchise_group,
	franchise::varchar(50) as franchise,
	product_franchise::varchar(50) as product_franchise,
	product_major::varchar(50) as product_major,
	product_minor::varchar(50) as product_minor,
	current_sales_employee::varchar(50) as current_sales_employee,
	customer_number::number(18,0) as customer_number,
	local_customer_grp_1::varchar(50) as local_customer_grp_1,
	local_customer_grp_2::varchar(50) as local_customer_grp_2,
	local_customer_grp_3::varchar(50) as local_customer_grp_3,
	local_customer_grp_4::varchar(50) as local_customer_grp_4,
	local_customer_grp_5::varchar(50) as local_customer_grp_5,
	local_customer_grp_6::varchar(50) as local_customer_grp_6,
	sales_target::number(20,5) as sales_target,
	quantity::number(18,0) as quantity,
	unit::varchar(50) as unit,
	fiscal_variant::varchar(5) as fiscal_variant,
	fiscal_year::number(18,0) as fiscal_year,
	country::varchar(5) as country,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final