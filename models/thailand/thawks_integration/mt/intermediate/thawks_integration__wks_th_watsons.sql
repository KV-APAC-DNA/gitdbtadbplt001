{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_mt_watsons') }}
),

final as (
    select
    div::varchar(50) as div,
	dept::varchar(50) as dept,
	class::varchar(50) as class,
	subclass::varchar(100) as subclass,
	item::varchar(100) as item,
	item_desc::varchar(200) as item_desc,
	non_slow::varchar(100) as non_slow,
	non_slow2::varchar(100) as non_slow2,
	finance_status::varchar(200) as finance_status,
	create_datetime::varchar(50) as create_datetime,
	prim_supplier::varchar(100) as prim_supplier,
	old_supp_no::varchar(100) as old_supp_no,
	supp_desc::varchar(200) as supp_desc,
	lead_time::varchar(50) as lead_time,
	unit_cost::varchar(100) as unit_cost,
	unit_retail_zone5::varchar(100) as unit_retail_zone5,
	item_status::varchar(100) as item_status,
	status_wh::varchar(100) as status_wh,
	status_wh_update_date::varchar(50) as status_wh_update_date,
	status_store::varchar(100) as status_store,
	status_store_update_date::varchar(50) as status_store_update_date,
	status_xdock::varchar(50) as status_xdock,
	status_xdock_update_date::varchar(50) as status_xdock_update_date,
	source_method::varchar(100) as source_method,
	source_wh::varchar(100) as source_wh,
	pog::varchar(100) as pog,
	product_type::varchar(200) as product_type,
	label_uda::varchar(100) as label_uda,
	brand::varchar(100) as brand,
	item_type::varchar(100) as item_type,
	return_policy::varchar(100) as return_policy,
	return_type::varchar(100) as return_type,
	wh_wac::varchar(100) as wh_wac,
	in_tax::varchar(50) as in_tax,
	tax_rate::varchar(50) as tax_rate,
	stock_cat::varchar(50) as stock_cat,
	order_flag::varchar(50) as order_flag,
	new_item_13week::varchar(50) as new_item_13week,
	deactivate_date::varchar(50) as deactivate_date,
	wh_on_order::varchar(50) as wh_on_order,
	first_rcv::varchar(50) as first_rcv,
	promo_month::varchar(50) as promo_month,
	sales_tw::varchar(50) as sales_tw,
	net_amt::varchar(50) as net_amt,
	net_cost::varchar(50) as net_cost,
	sale_avg_qty_13weeks::number(16,4) as sale_avg_qty_13weeks,
	sale_avg_amt_13weeks::number(16,4) as sale_avg_amt_13weeks,
	sale_avg_cost13weeks::number(16,4) as sale_avg_cost13weeks,
	net_qty_ytd::number(16,4) as net_qty_ytd,
	net_amt_ytd::number(16,4) as net_amt_ytd,
	net_cost_ytd::number(16,4) as net_cost_ytd,
	turn_wk::varchar(100) as turn_wk,
	wh_soh::number(16,4) as wh_soh,
	store_total_stock::number(16,4) as store_total_stock,
	total_stock_qty::number(16,4) as total_stock_qty,
	wh_stock_amt::number(16,4) as wh_stock_amt,
	store_total_stock_amt::number(16,4) as store_total_stock_amt,
	total_stock_xvat::number(16,4) as total_stock_xvat,
	pro2::varchar(50) as pro2,
	disc::varchar(50) as disc,
	pro22::varchar(50) as pro22,
	pro2_pert_disc::varchar(50) as pro2_pert_disc,
	first_date_sms::varchar(50) as first_date_sms,
	aging_sms::varchar(50) as aging_sms,
	group_w::varchar(50) as group_w,
	win::varchar(50) as win,
	pog_2::varchar(50) as pog_2,
	file_name::varchar(100) as file_name,
	date::varchar(10) as date,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
    -- {% if is_incremental() %}
    --     where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    -- {% endif %}
)

select * from final