{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['transaction_date']
    )
}}

with  sdl_th_mt_bigc as (
select * from {{ source('thasdl_raw', 'sdl_th_mt_bigc') }}
),
final as (
select
report_code::varchar(100) as report_code,
supplier::varchar(500) as supplier,
business_format::varchar(500) as business_format,
compare::varchar(500) as compare,
store::varchar(500) as store,
transaction_date::varchar(20) as transaction_date,
ly_compare_date::varchar(20) as ly_compare_date,
report_date::varchar(20) as report_date,
division::varchar(500) as division,
department::varchar(500) as department,
subdepartment::varchar(500) as subdepartment,
class::varchar(500) as class,
subclass::varchar(500) as subclass,
barcode::varchar(500) as barcode,
article::varchar(500) as article,
article_name::varchar(500) as article_name,
brand::varchar(500) as brand,
model::varchar(500) as model,
sale_amt_ty_baht::number(16,4) as sale_amt_ty_baht,
sale_amt_ly_baht::number(16,4) as sale_amt_ly_baht,
sale_amt_var::number(16,4) as sale_amt_var,
sale_qty_ty::number(16,4) as sale_qty_ty,
sale_qty_ly::number(16,4) as sale_qty_ly,
sale_qty_var::number(16,4) as sale_qty_var,
stock_ty_baht::number(16,4) as stock_ty_baht,
stock_ly_baht::number(16,4) as stock_ly_baht,
stock_var::number(16,4) as stock_var,
stock_qty_ty::number(16,4) as stock_qty_ty,
stock_qty_ly::number(16,4) as stock_qty_ly,
stock_qty_var::number(16,4) as stock_qty_var,
day_on_hand_ty::number(16,4) as day_on_hand_ty,
day_on_hand_ly::number(16,4) as day_on_hand_ly,
day_on_hand_diff::number(16,4) as day_on_hand_diff,
file_name::varchar(200) as file_name,
crt_dttm::timestamp_ntz(9) as crt_dttm
from sdl_th_mt_bigc)
select * from final