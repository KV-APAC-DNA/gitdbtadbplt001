{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with sdl_th_mt_makro as (
select * from {{ source('thasdl_raw', 'sdl_th_mt_makro') }}
),
final as (
select
transaction_date::varchar(50) as transaction_date,
supplier_number::varchar(50) as supplier_number,
location_number::varchar(100) as location_number,
location_name::varchar(500) as location_name,
class_number::varchar(100) as class_number,
subclass_number::varchar(100) as subclass_number,
item_number::varchar(100) as item_number,
barcode::varchar(100) as barcode,
item_desc::varchar(500) as item_desc,
eoh_qty::varchar(50) as eoh_qty,
order_in_transit_qty::varchar(50) as order_in_transit_qty,
pack_type::varchar(50) as pack_type,
makro_unit::varchar(50) as makro_unit,
avg_net_sales_qty::varchar(50) as avg_net_sales_qty,
net_sales_qty_ytd::varchar(50) as net_sales_qty_ytd,
last_recv_dt::varchar(50) as last_recv_dt,
last_sold_dt::varchar(50) as last_sold_dt,
stock_cover_days::varchar(20) as stock_cover_days,
net_sales_qty_mtd::varchar(50) as net_sales_qty_mtd,
day_1::varchar(50) as day_1,
day_2::varchar(50) as day_2,
day_3::varchar(50) as day_3,
day_4::varchar(50) as day_4,
day_5::varchar(50) as day_5,
day_6::varchar(50) as day_6,
day_7::varchar(50) as day_7,
day_8::varchar(50) as day_8,
day_9::varchar(50) as day_9,
day_10::varchar(50) as day_10,
day_11::varchar(50) as day_11,
day_12::varchar(50) as day_12,
day_13::varchar(50) as day_13,
day_14::varchar(50) as day_14,
day_15::varchar(50) as day_15,
day_16::varchar(50) as day_16,
day_17::varchar(50) as day_17,
day_18::varchar(50) as day_18,
day_19::varchar(50) as day_19,
day_20::varchar(50) as day_20,
day_21::varchar(50) as day_21,
day_22::varchar(50) as day_22,
day_23::varchar(50) as day_23,
day_24::varchar(50) as day_24,
day_25::varchar(50) as day_25,
day_26::varchar(50) as day_26,
day_27::varchar(50) as day_27,
day_28::varchar(50) as day_28,
day_29::varchar(50) as day_29,
day_30::varchar(50) as day_30,
day_31::varchar(50) as day_31,
file_name::varchar(100) as file_name,
crtd_dtm::timestamp_ntz(9) as crtd_dtm,
from sdl_th_mt_makro
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %} 
 )
select * from final