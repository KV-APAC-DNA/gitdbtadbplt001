with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_search_ads_by_channel') }}
),
final as
( 
	select 
		"Time_Period"::varchar(100) as time_period,
		"Channel"::varchar(255) as channel,
		"Region"::varchar(100) as region,
		"Shop_Name"::varchar(255) as shop_name,
		"Shop_ID"::varchar(255) as shop_id,
		"Terminal"::varchar(50) as terminal,
		"Sales_usd"::number(38,5) as sales_usd,
		"Sales_lcy"::number(38,5) as sales_lcy,
		"Orders"::integer as orders,
		"Units_Sold"::integer as units_sold,
		"Visits"::integer as visits,
		"Unique_Visitors"::integer as unique_visitors,
		"Add_To_Cart_Units"::number(38,0) as cart_units,
		"Add_To_Cart_Value_USD"::number(38,5) as cart_value_usd,
		"Add_To_Cart_Value_lcy"::number(38,5) as cart_value_lcy,
		"Buyers"::integer as buyers,
		"New_Buyers"::integer as new_buyers,
		"Item_Conversion_Rate"::number(38,5) as item_conversion_rate,
		"FILE_NAME"::varchar(255) as filename,
		"CRTD_DTTM" :: timestamp_ntz(9) as crtd_dttm,	
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final