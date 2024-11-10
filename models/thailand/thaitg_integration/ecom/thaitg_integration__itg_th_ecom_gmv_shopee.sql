with source_1 as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_gmv_shopee_hb') }}
),
source_2 as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_gmv_shopee_jb') }}
),
source_3 as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_gmv_shopee_ltr') }}
),
final as
(
select
	product_name::varchar(400) as product_name,
    product_id::varchar(255) as product_id,
    gross_sales::number(20,4) as gross_sales,
    gross_sales_growth::number(20,4) as gross_sales_growth,
    gross_orders::number(20,4) as gross_orders,
    gross_orders_growth::number(20,4) as gross_orders_growth,
    gross_units_sold::number(20,4) as gross_units_sold,
    gross_units_sold_growth::number(20,4) as gross_units_sold_growth,
    gross_avg_basket_size::number(20,4) as gross_avg_basket_size,
    gross_avg_basket_size_growth::number(20,4) as gross_avg_basket_size_growth,
    gross_item_per_order::number(20,4) as gross_item_per_order,
    gross_item_per_order_growth::number(20,4) as gross_item_per_order_growth,
    gross_avg_selling_price::number(20,4) as gross_avg_selling_price,
    gross_avg_selling_price_growth::number(20,4) as gross_avg_selling_price_growth,
    product_views::number(20,4) as product_views,
    product_views_growth::number(20,4) as product_views_growth,
    product_visitors::number(20,4) as product_visitors,
    product_visitors_growth::number(20,4) as product_visitors_growth,
    gross_order_conversion_rate::number(20,4) as gross_order_conversion_rate,
    gross_order_conversion_rate_growth::number(20,4) as gross_order_conversion_rate_growth,
    gross_item_conversion_rate::number(20,4) as gross_item_conversion_rate,
    gross_item_conversion_rate_growth::number(20,4) as gross_item_conversion_rate_growth,
    date::varchar(50) as date,
    platform::varchar(50) as platform,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_1
UNION ALL
select
	product_name::varchar(400) as product_name,
    product_id::varchar(255) as product_id,
    gross_sales::number(20,4) as gross_sales,
    gross_sales_growth::number(20,4) as gross_sales_growth,
    gross_orders::number(20,4) as gross_orders,
    gross_orders_growth::number(20,4) as gross_orders_growth,
    gross_units_sold::number(20,4) as gross_units_sold,
    gross_units_sold_growth::number(20,4) as gross_units_sold_growth,
    gross_avg_basket_size::number(20,4) as gross_avg_basket_size,
    gross_avg_basket_size_growth::number(20,4) as gross_avg_basket_size_growth,
    gross_item_per_order::number(20,4) as gross_item_per_order,
    gross_item_per_order_growth::number(20,4) as gross_item_per_order_growth,
    gross_avg_selling_price::number(20,4) as gross_avg_selling_price,
    gross_avg_selling_price_growth::number(20,4) as gross_avg_selling_price_growth,
    product_views::number(20,4) as product_views,
    product_views_growth::number(20,4) as product_views_growth,
    product_visitors::number(20,4) as product_visitors,
    product_visitors_growth::number(20,4) as product_visitors_growth,
    gross_order_conversion_rate::number(20,4) as gross_order_conversion_rate,
    gross_order_conversion_rate_growth::number(20,4) as gross_order_conversion_rate_growth,
    gross_item_conversion_rate::number(20,4) as gross_item_conversion_rate,
    gross_item_conversion_rate_growth::number(20,4) as gross_item_conversion_rate_growth,
    date::varchar(50) as date,
    platform::varchar(50) as platform,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_2
UNION ALL
select
	product_name::varchar(400) as product_name,
    product_id::varchar(255) as product_id,
    gross_sales::number(20,4) as gross_sales,
    gross_sales_growth::number(20,4) as gross_sales_growth,
    gross_orders::number(20,4) as gross_orders,
    gross_orders_growth::number(20,4) as gross_orders_growth,
    gross_units_sold::number(20,4) as gross_units_sold,
    gross_units_sold_growth::number(20,4) as gross_units_sold_growth,
    gross_avg_basket_size::number(20,4) as gross_avg_basket_size,
    gross_avg_basket_size_growth::number(20,4) as gross_avg_basket_size_growth,
    gross_item_per_order::number(20,4) as gross_item_per_order,
    gross_item_per_order_growth::number(20,4) as gross_item_per_order_growth,
    gross_avg_selling_price::number(20,4) as gross_avg_selling_price,
    gross_avg_selling_price_growth::number(20,4) as gross_avg_selling_price_growth,
    product_views::number(20,4) as product_views,
    product_views_growth::number(20,4) as product_views_growth,
    product_visitors::number(20,4) as product_visitors,
    product_visitors_growth::number(20,4) as product_visitors_growth,
    gross_order_conversion_rate::number(20,4) as gross_order_conversion_rate,
    gross_order_conversion_rate_growth::number(20,4) as gross_order_conversion_rate_growth,
    gross_item_conversion_rate::number(20,4) as gross_item_conversion_rate,
    gross_item_conversion_rate_growth::number(20,4) as gross_item_conversion_rate_growth,
    date::varchar(50) as date,
    platform::varchar(50) as platform,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source_3
)
select * from final