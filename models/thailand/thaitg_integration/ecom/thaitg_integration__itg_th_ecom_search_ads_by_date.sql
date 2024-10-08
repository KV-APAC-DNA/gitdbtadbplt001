with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_search_ads_by_date') }}
),
final as
( 
	select 
		DATE::VARCHAR(100) as DATE,
		REGION::VARCHAR(100) as REGION,
		SHOP_NAME::VARCHAR(255) as SHOP_NAME,
		SHOP_ID::VARCHAR(255) as SHOP_ID,
		TERMINAL::VARCHAR(50) as TERMINAL,
		SALES_USD::NUMBER(38,5) as SALES_USD,
		SALES_LCY::NUMBER(38,5) as SALES_LCY,
		ORDERS::integer as ORDERS,
		UNITS_SOLD::integer as UNITS_SOLD,
		VISITS::integer as VISITS,
		UNIQUE_VISITORS::integer as UNIQUE_VISITORS,
		ADD_TO_CART_UNITS::NUMBER(38,0) as CART_UNITS,
		ADD_TO_CART_VALUE_USD::NUMBER(38,5) as CART_VALUE_USD,
		ADD_TO_CART_VALUE_LCY::NUMBER(38,5) as CART_VALUE_LCY,
		BUYERS::integer as BUYERS,
		NEW_BUYERS::integer as NEW_BUYERS,
		ITEM_CONVERSION_RATE::NUMBER(38,5) as ITEM_CONVERSION_RATE,
		FILENAME::VARCHAR(255) as filename,
		CRTD_DTTM :: TIMESTAMP_NTZ(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final

