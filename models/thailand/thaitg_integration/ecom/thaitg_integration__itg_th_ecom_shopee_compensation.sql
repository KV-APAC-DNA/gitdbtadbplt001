{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['ORDER_NO'],
        pre_hook = "delete from {{this}} where SCM_BARCODE||ORDER_NO in (
        select distinct SCM_BARCODE||ORDER_NO from {{ source('thasdl_raw', 'sdl_ecom_shopee_compensation') }} );"
    )
}} 
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_shopee_compensation') }}
),
final as
( 
	select 
		PO_CMPT_MNTH::VARCHAR(50) as PO_CMPT_MNTH,
		TYPE::VARCHAR(100) as type,
		SCM_BARCODE::VARCHAR(100) as SCM_BARCODE,
		PRODUCT_NAME::VARCHAR(255) as PRODUCT_NAME,
		ORDER_NO::VARCHAR(255) as ORDER_NO,
		STATUS::VARCHAR(50) as STATUS,
		CRT_TM::VARCHAR(100) as CRT_TM,
		DLVRY_TM::VARCHAR(100) as DLVRY_TM,
		SUM_OF_QTY::integer as QTY,
		SUM_OF_RSPxQTY::integer as RSP_QTY,
		SUM_OF_TOTAL_NET_SELLING_PRICE::integer as total_net_selling_price,
		SUM_OF_DIS_SHOPEE::integer as dis_shopee,
		SUM_OF_LTPxQTY::integer as ltp_qty,
		SUM_OF_FRONT_MARGIN::integer as front_margin,
		SUM_OF_ENABLER_MARGIN::integer as enabler_margin,
		SUM_OF_COMPENSATION::integer as compensation,
		FILENAME::VARCHAR(255) as filename,
		CRTD_DTTM :: TIMESTAMP_NTZ(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final