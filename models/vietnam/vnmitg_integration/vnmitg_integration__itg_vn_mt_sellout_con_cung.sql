{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}

with source as (
    select * from {{ source('snaposesdl_raw','sdl_vn_mt_sellout_con_cung') }}
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
	delivery_code::varchar(50) as delivery_code,
	store::varchar(255) as store,
	product_code::varchar(20) as product_code,
	product_name::varchar(255) as product_name,
	quantity::number(18,0) as quantity,
	filename::varchar(100) as filename,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final