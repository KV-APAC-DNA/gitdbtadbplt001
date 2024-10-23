{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        unique_keys=['date','shop'],
        pre_hook = "delete from {{this}} where filename in (
        select distinct filename from {{ source('thasdl_raw', 'sdl_ecom_cpas') }} );"
    )
}}
with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_cpas') }}
),
final as
(
	select 
		date::varchar(50) as date,
		shop::varchar(50) as shop,
		spending::number(20,4) as spending,
		filename::varchar(255) as filename,
		crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm
	from source
)
select * from final