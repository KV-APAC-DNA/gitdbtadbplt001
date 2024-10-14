{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "delete from {{this}} where DATE||SHOP in (
        select distinct DATE||SHOP from {{ source('thasdl_raw', 'sdl_ecom_cpas') }} );"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_cpas') }}
),
final as
(
	Select 
		DATE::VARCHAR(50) as date,
		SHOP::VARCHAR(50) as shop,
		SPENDING::NUMBER(20,4) as spending,
		FILENAME::VARCHAR(255) as filename,
		CRTD_DTTM :: TIMESTAMP_NTZ(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final