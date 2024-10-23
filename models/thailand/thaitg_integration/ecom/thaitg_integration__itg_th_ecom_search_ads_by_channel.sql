{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['time_period','channel','region','terminal','shop_name'],
        pre_hook = "delete from {{this}} where filename in (
        select distinct file_name from {{ source('thasdl_raw', 'sdl_ecom_search_ads_by_channel') }} );"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_search_ads_by_channel') }}
),
final as
( 
	select 
		time_period::varchar(100) as time_period,
		channel::varchar(255) as channel,
		region::varchar(100) as region,
		shop_name::varchar(255) as shop_name,
		shop_id::varchar(255) as shop_id,
		terminal::varchar(50) as terminal,
		sales_usd::number(38,5) as sales_usd,
		sales_lcy::number(38,5) as sales_lcy,
		orders::integer as orders,
		units_sold::integer as units_sold,
		visits::integer as visits,
		unique_visitors::integer as unique_visitors,
		add_to_cart_units::number(38,0) as cart_units,
		add_to_cart_value_usd::number(38,5) as cart_value_usd,
		add_to_cart_value_lcy::number(38,5) as cart_value_lcy,
		buyers::integer as buyers,
		new_buyers::integer as new_buyers,
		item_conversion_rate::number(38,5) as item_conversion_rate,
		file_name::varchar(255) as filename,
		crtd_dttm :: timestamp_ntz(9) as crtd_dttm,	
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final