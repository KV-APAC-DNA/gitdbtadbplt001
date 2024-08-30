{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}

with source as (
    select *, dense_rank() over (partition by null order by filename desc) rnk
     from {{ source('vnmsdl_raw','sdl_vn_mt_sellout_guardian') }}
    where filename not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_mt_sellout_guardian__null_test')}}
        ) qualify rnk = 1
),

final as
(
    select
    substring(right(filename,11),4,4)::varchar(20) as year,
    case
    when substring(right (filename, 11), 1, 3) LIKE '%Jan%' then '01'
    when substring(right (filename, 11), 1, 3) LIKE '%Feb%' then '02'
    when substring(right (filename, 11), 1, 3) LIKE '%Mar%' then '03'
    when substring(right (filename, 11), 1, 3) LIKE '%Apr%' then '04'
    when substring(right (filename, 11), 1, 3) LIKE '%May%' then '05'
    when substring(right (filename, 11), 1, 3) LIKE '%Jun%' then '06'
    when substring(right (filename, 11), 1, 3) LIKE '%Jul%' then '07'
    when substring(right (filename, 11), 1, 3) LIKE '%Aug%' then '08'
    when substring(right (filename, 11), 1, 3) LIKE '%Sep%' then '09'
    when substring(right (filename, 11), 1, 3) LIKE '%Oct%' then '10'
    when substring(right (filename, 11), 1, 3) LIKE '%Nov%' then '11'
    when substring(right (filename, 11), 1, 3) LIKE '%Dec%' then '12'
end::varchar(20) as month,
    serial_no::number(18,0) as serial_no,
	store_code::varchar(20) as store_code,
	store_name::varchar(255) as store_name,
	sku::varchar(20) as sku,
	barcode::varchar(20) as barcode,
	description_vietnamese::varchar(255) as description_vietnamese,
	brand::varchar(100) as brand,
	division::varchar(50) as division,
	department::varchar(50) as department,
	category::varchar(50) as category,
	sub_category::varchar(50) as sub_category,
	sales_supplier::number(18,0) as sales_supplier,
	amount::number(20,5) as amount,
	filename::varchar(100) as filename,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
)
select * from final