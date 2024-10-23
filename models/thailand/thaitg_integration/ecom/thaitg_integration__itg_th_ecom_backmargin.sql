{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['ordersn','shop_id'],
        pre_hook = "delete from {{this}} where filename in (
        select distinct filename from {{ source('thasdl_raw', 'sdl_ecom_backmargin') }} );"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_backmargin') }}
),
final as
( 
	select 
		ordersn::varchar(100) as ordersn,
		shop_id::varchar(50) as shop_id,
		seller_username::varchar(100) as seller_username,
		sku_id::varchar(100) as sku_id,
		bundle_ord_itmid::varchar(10) as bundle_ord_itmid,
		product_id::varchar(100) as product_id,
		itm_name::varchar(16777216) as itm_name,
		model_id::varchar(100) as model_id,
		model_name::varchar(500) as model_name,
		purchased_time::varchar(20) as purchased_time,
		category::varchar(100) as category,
		sub_category::varchar(100) as sub_category,
		be_status::varchar(100) as be_status,
		seller_voucher::varchar(100) as seller_voucher,
		platform_voucher::varchar(100) as platform_voucher,
		quantity::number(38,0) as quantity,
		flash_sale::varchar(100) as flash_sale,
		free_shipping_rebate::number(20,4) as free_shipping_rebate,
		shopee_voucher_rebate::number(20,4) as shopee_voucher_rebate,
		seller_voucher_rebate::number(20,4) as seller_voucher_rebate,
		platform_voucher_seller_rebate::number(20,4) as platform_voucher_seller_rebate,
		platform_voucher_shopee_rebate::number(20,4) as platform_voucher_shopee_rebate,
		coin_earn::number(20,4) as coin_earn,
		coin_used::number(20,4) as coin_used,
		specific_purchase_time::varchar(50) as specific_purchase_time,
		cogs_per_quantity::number(20,4) as cogs_per_quantity,
		price_before_discount::number(20,4) as price_before_discount,
		item_price::number(20,4) as item_price,
		percent_discount::number(20,4) as percent_discount,
		promotion_id::varchar(100) as promotion_id,
		shipping_carrier::varchar(255) as shipping_carrier,
		bundle_id::varchar(100) as bundle_id,
		actual_shipping_fee::number(20,4) as actual_shipping_fee,
		buyer_shipping_fee::number(20,4) as buyer_shipping_fee,
		add_on_deal_id::varchar(100) as add_on_deal_id,
		is_add_on_sub_item::number(38,0) as is_add_on_sub_item,
		backmargin_cfs::number(20,4) as backmargin_cfs,
		backmargin_normal::number(20,4) as backmargin_normal,
		backmargin_bundle::number(20,4) as backmargin_bundle,
		backmargin_addon::number(20,4) as backmargin_addon,
		backmargin_shipping::number(20,4) as backmargin_shipping,
		seller_voucher_coin_earn::number(20,4) as seller_voucher_coin_earn,
		platform_voucher_coin_earn::number(20,4) as platform_voucher_coin_earn,
		supplier_id::varchar(100) as supplier_id,
		suppliername::varchar(255) as suppliername,
		nmv::number(20,4) as nmv,
		bundle_rule_type::varchar(100) as bundle_rule_type,
		bundle_rule_value::varchar(100) as bundle_rule_value,
		item_brand::varchar(100) as item_brand,
		is_exclusive_price::number(38,0) as is_exclusive_price,
		order_value::number(20,4) as order_value,
		is_new_buyer_price::number(38,0) as is_new_buyer_price,
		is_live_order::number(38,0) as is_live_order,
		escrow_paid_date::varchar(50) as escrow_paid_date,
		filename::varchar(255) as filename,
		crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final