with 
edw_rpt_sales_details as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
trans as 
(
    select 
    'INDIA' as country,
    sales1.region_name,
    sales1.zone_name,
    sales1.territory_name,
    sales1.channel_name,
    sales1.retail_environment,
    sales1.salesman_code,
    sales1.salesman_name,
    sales1.distributor_code,
    sales1.distributor_name,
    sales1.qtr,
    sales1.cal_yr,
    sales1.store_code,
(
        sales_ret.rtruniquecode || '-' || upper (sales_ret.retailer_name)
    ) as store_name,
    sales1.rtruniquecode,
    sales1.prod_hier_l3,
    sales1.prod_hier_l4,
    sales1.prod_hier_l5,
    sales1.prod_hier_l6,
    sales1.prod_hier_l7,
    sales1.prod_hier_l8,
    sales1.product_code,
    sales1.prod_hier_l9,
    sum(sales1.achievement_nr) as achievement_nr
from (
        select sales.region_name,
            sales.zone_name,
            sales.territory_name,
            sales.channel_name,
            sales.retailer_category_name as retail_environment,
            sales.salesman_code,
            sales.salesman_name,
            sales.customer_code as distributor_code,
            sales.customer_name as distributor_name,
            sales.qtr,
            sales.cal_yr,
            sales.retailer_code as store_code,
            sales.rtruniquecode,
case
                when upper(sales.franchise_name) = 'SANPRO' THEN 'STAYFREE'
                when upper(sales.franchise_name) = 'OTC' THEN 'LISTERINE'
                when upper(sales.franchise_name) = 'BEAUTY CARE' THEN 'C&C'
                when upper(sales.franchise_name) = 'BABY CARE' THEN 'JOHNSON BABY'
            end as prod_hier_l3,
            sales.brand_name as prod_hier_l4,
            sales.product_category_name as prod_hier_l5,
            sales.variant_name as prod_hier_l6,
            null as prod_hier_l7,
            null as prod_hier_l8,
            sales.mothersku_name as product_code,
            sales.mothersku_name as prod_hier_l9,
            sales.achievement_nr 
        from edw_rpt_sales_details sales
        where upper(sales.channel_name) = 'SELF SERVICE STORE'
    ) sales1
    left outer join (
        select distinct rtruniquecode,
            b.retailer_code,
            b.retailer_name,
            row_number() over (
                partition by b.retailer_code
                order by invoice_date desc
            ) as rn
        from edw_rpt_sales_details b
        where upper(b.channel_name) = 'SELF SERVICE STORE'
    ) sales_ret on sales1.store_code = sales_ret.retailer_code
    and sales_ret.rn = 1
group by sales1.region_name,
    sales1.zone_name,
    sales1.territory_name,
    sales1.channel_name,
    sales1.retail_environment,
    sales1.salesman_code,
    sales1.salesman_name,
    sales1.distributor_code,
    sales1.distributor_name,
    sales1.qtr,
    sales1.cal_yr,
    sales1.store_code,
    store_name,
    sales1.rtruniquecode,
    sales1.prod_hier_l3,
    sales1.prod_hier_l4,
    sales1.prod_hier_l5,
    sales1.prod_hier_l6,
    sales1.prod_hier_l7,
    sales1.prod_hier_l8,
    sales1.product_code,
    sales1.prod_hier_l9
),
final as 
(
    select 
    country::varchar(5) as country,
	region_name::varchar(50) as region_name,
	zone_name::varchar(50) as zone_name,
	territory_name::varchar(50) as territory_name,
	channel_name::varchar(150) as channel_name,
	retail_environment::varchar(50) as retail_environment,
	salesman_code::varchar(100) as salesman_code,
	salesman_name::varchar(200) as salesman_name,
	distributor_code::varchar(50) as distributor_code,
	distributor_name::varchar(150) as distributor_name,
	qtr::number(18,0) as qtr,
	cal_yr::number(18,0) as cal_yr,
	store_code::varchar(100) as store_code,
	store_name::varchar(251) as store_name,
	rtruniquecode::varchar(100) as rtruniquecode,
	prod_hier_l3::varchar(12) as prod_hier_l3,
	prod_hier_l4::varchar(50) as prod_hier_l4,
	prod_hier_l5::varchar(150) as prod_hier_l5,
	prod_hier_l6::varchar(150) as prod_hier_l6,
	prod_hier_l7::varchar(1) as prod_hier_l7,
	prod_hier_l8::varchar(1) as prod_hier_l8,
	product_code::varchar(150) as product_code,
	prod_hier_l9::varchar(150) as prod_hier_l9,
	achievement_nr::number(38,6) as achievement_nr
    from trans
)
select * from final