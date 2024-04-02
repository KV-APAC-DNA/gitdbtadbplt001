{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_vn_mt_sellout_lotte') }}
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
    str::varchar(10) as str,
	str_nm::varchar(50) as str_nm,
	cat_nm_1::varchar(50) as cat_nm_1,
	cat_nm_2::varchar(50) as cat_nm_2,
	cat_nm_3::varchar(50) as cat_nm_3,
	cat_nm_4::varchar(50) as cat_nm_4,
	prod_cd::varchar(100) as prod_cd,
	sale_cd::varchar(100) as sale_cd,
	prod_nm::varchar(100) as prod_nm,
	ven::varchar(100) as ven,
	ven_nm::varchar(100) as ven_nm,
	sale_qty::number(18,0) as sale_qty,
	tot_sale_amt::number(20,5) as tot_sale_amt,
	filename::varchar(100) as filename,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    from source
)
select * from final