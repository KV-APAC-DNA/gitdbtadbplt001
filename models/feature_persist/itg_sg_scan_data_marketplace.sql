{{
    config(
        materialized='incremental',
        incremental_strategy='delet+insert',
        unique_key=['trx_date']
    )
}}

with sdl_sg_scan_data_marketplace as (
    select 
    "channel" as channel,
    "month" as month,
    "order_creation_date" as order_creation_date,
    "invoice_number" as invoice_number,
    "status" as status,
    "item_code" as item_code,
    "item_name" as item_name,
    "sales_unit" as sales_unit,
    "net_invoiced_sales" as net_invoiced_sales,
    "brand" as brand,
    "cust_name" as cust_name,
    "cdl_dttm" as cdl_dttm,
    "crtd_dttm" as crtd_dttm,
    "file_name" as file_name,
    "run_id" as run_id
    from {{ source('ose_raw', 'sdl_sg_scan_data_marketplace') }}
),
final as (
    select 
    order_creation_date as trx_date,
	left(trx_date, 4) as year,
	null as mnth_id,
	null as week,
	channel as store_name,
	month,
	invoice_number,
	status,
	item_code,
	item_name as item_desc,
	'NA' as barcode,
	sales_unit as sales_qty,
	net_invoiced_sales as net_sales,
	brand,
	cdl_dttm,
	crtd_dttm,
	current_timestamp() as updt_dttm,
	file_name,
	run_id,
	cust_name
    from sdl_sg_scan_data_marketplace
)

select * from final 