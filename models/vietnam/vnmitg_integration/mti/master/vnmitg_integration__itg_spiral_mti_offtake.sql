{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where ((CustomerName,ShopCode,COALESCE(SupCode,''),Year,Month,BarCode) IN (select CustomerName,ShopCode,COALESCE(SupCode,''),Year,Month,BarCode
                                       from {{ source('vnmsdl_raw','sdl_spiral_mti_offtake') }} 
                                       where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_spiral_mti_offtake__null_test')}}));
        {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_spiral_mti_offtake') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_spiral_mti_offtake__null_test')}}
    )
),

final as
(
    select
    stt::varchar(1000) as stt,
	area::varchar(1000) as area,
	channelname::varchar(1000) as channelname,
	customername::varchar(1000) as customername,
	shopcode::varchar(1000) as shopcode,
	shopname::varchar(1000) as shopname,
	address::varchar(1000) as address,
	supcode::varchar(10000) as supcode,
	supname::varchar(1000) as supname,
	year::varchar(1000) as year,
	month::varchar(1000) as month,
	barcode::varchar(1000) as barcode,
	productname::varchar(1000) as productname,
	franchise::varchar(1000) as franchise,
	brand::varchar(1000) as brand,
	cate::varchar(1000) as cate,
	sub_cat::varchar(1000) as sub_cat,
	sub_brand::varchar(1000) as sub_brand,
	size::varchar(1000) as size,
	quantity::varchar(1000) as quantity,
	amount::varchar(1000) as amount,
	amountusd::varchar(1000) as amountusd,
	file_name::varchar(1000) as file_name,
	current_timestamp()::timestamp_ntz(9) as crtd_name,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final