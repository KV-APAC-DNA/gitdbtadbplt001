with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_tw_ims_dstr_prod_price_map') }}
),
final as
(
    select
		dstr_code::varchar(10) as dstr_cd,
		dstr_name::varchar(20) as dstr_nm,
		ean_code::varchar(20) as ean_cd,
		dstr_product_code::varchar(20) as dstr_prod_cd,
		dstr_product_name::varchar(200) as dstr_prod_nm,
		sell_out_price_manual::number(30,4) as sell_out_price_manual,
		to_date(promotion_start_date) as promotion_start_date,
		to_date(promotion_end_date) as promotion_end_date,
		current_timestamp()::timestamp_ntz(9) as crt_dttm,
		current_timestamp()::timestamp_ntz(9) as updt_dttm      
    from source
)
select * from final