{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trx_date']
    )
}}

--import CTE
with source as (
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_marketplace') }}
),

edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),

--logical CTE
final as (
select
  order_creation_date::date as trx_date,
  left(trx_date, 4)::varchar(20) as year,
  cal.cal_mo_1::varchar(23) as mnth_id,
  cal.cal_wk::varchar(20) as week,
  channel::varchar(20) as store_name,
  month::varchar(20) as month,
  invoice_number::varchar(255) as invoice_number,
  status::varchar(100) as status,
  item_code::varchar(500) as item_code,
  item_name::varchar(500) as item_desc,
  'NA'::varchar(255) as barcode,
  sales_unit::number(10, 0) as sales_qty,
  net_invoiced_sales::number(10, 4) as net_sales,
  brand::varchar(300) as brand,
  cdl_dttm::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm,
  file_name::varchar(255) as file_name,
  run_id::number(14, 0) as run_id,
  cust_name::varchar(20) as cust_name
from source as sg_mkpl
left join edw_calendar_dim as cal
  on sg_mkpl.order_creation_date = cal.cal_day

)

select * from final