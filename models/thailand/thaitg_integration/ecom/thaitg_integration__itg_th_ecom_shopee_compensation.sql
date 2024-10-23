{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['scm_barcode','order_no'],
        pre_hook = "delete from {{this}} where filename in (
        select distinct filename from {{ source('thasdl_raw', 'sdl_ecom_shopee_compensation') }} );"
    )
}} 
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_shopee_compensation') }}
),
final as
( 
	select 
		po_cmpt_mnth::varchar(50) as po_cmpt_mnth,
		type::varchar(100) as type,
		scm_barcode::varchar(100) as scm_barcode,
		product_name::varchar(255) as product_name,
		order_no::varchar(255) as order_no,
		status::varchar(50) as status,
		crt_tm::varchar(100) as crt_tm,
		dlvry_tm::varchar(100) as dlvry_tm,
		sum_of_qty::integer as qty,
		sum_of_rspxqty::integer as rsp_qty,
		sum_of_total_net_selling_price::integer as total_net_selling_price,
		sum_of_dis_shopee::integer as dis_shopee,
		sum_of_ltpxqty::integer as ltp_qty,
		sum_of_front_margin::integer as front_margin,
		sum_of_enabler_margin::integer as enabler_margin,
		sum_of_compensation::integer as compensation,
		filename::varchar(255) as filename,
		crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final