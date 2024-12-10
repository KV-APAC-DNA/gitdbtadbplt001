with source as
(
    select * from {{ source('thasdl_raw', 'sdl_ecom_gmv_konvy') }}
),
final as
(
select
	teamid::varchar(50) as teamid,
    teamid2::varchar(50) as teamid2,
    brand::varchar(100) as brand,
    productname::varchar(400) as productname,
    status::varchar(50) as status,
    barcode::varchar(255) as barcode,
    itemcode::varchar(255) as itemcode,
    sales_amount::number(20,4) as sales_amount,
    total_unit_sold::number(20,4) as total_unit_sold,
    tstock::varchar(50) as tstock,
    current_price::number(20,4) as current_price,
    pro_price::number(20,4) as pro_price,
    pro_cost::number(20,4) as pro_cost,
    cost_in_vat::number(20,4) as cost_in_vat,
    cost::number(20,4) as cost,
    market_value::varchar(50) as market_value,
    stock_turnover_days::varchar(50) as stock_turnover_days,
    view_no ::varchar(50) as view_val,
    replace(conversion,'%','')::number(20,4) as conversion,
    discontinue::varchar(50) as discontinue,
    date::varchar(50) as date,
    platform::varchar(50) as platform,
	filename::varchar(255) as filename,
	crtd_dttm :: timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)
select * from final

