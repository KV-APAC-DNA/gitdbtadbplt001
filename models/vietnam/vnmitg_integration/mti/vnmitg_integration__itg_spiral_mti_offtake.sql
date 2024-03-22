{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['customername','shopcode','supcode','year','month','barcode']
    )
}}

with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_spiral_mti_offtake') }}
),

final as
(
    select
    stt::varchar(100) as stt,
	area::varchar(100) as area,
	channelname::varchar(100) as channelname,
	customername::varchar(100) as customername,
	shopcode::varchar(100) as shopcode,
	shopname::varchar(100) as shopname,
	address::varchar(100) as address,
	supcode::varchar(100) as supcode,
	supname::varchar(100) as supname,
	year::varchar(100) as year,
	month::varchar(100) as month,
	barcode::varchar(100) as barcode,
	productname::varchar(100) as productname,
	franchise::varchar(100) as franchise,
	brand::varchar(100) as brand,
	cate::varchar(100) as cate,
	sub_cat::varchar(100) as sub_cat,
	sub_brand::varchar(100) as sub_brand,
	size::varchar(100) as size,
	quantity::varchar(100) as quantity,
	amount::varchar(100) as amount,
	amountusd::varchar(100) as amountusd,
	file_name::varchar(100) as file_name,
	current_timestamp()::timestamp_ntz(9) as crtd_name,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final