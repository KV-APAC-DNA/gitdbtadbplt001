{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['transaction_date']
    )
}}
with sdl_th_mt_makro as (
  select * from {{ source('thasdl_raw', 'sdl_th_mt_makro') }}
),
final as (
select
  transaction_date,
  supplier_number,
  location_number,
  location_name,
  class_number,
  subclass_number,
  item_number,
  barcode,
  item_desc,
  cast(replace(replace(replace(eoh_qty, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as eoh_qty,
  cast(replace(replace(replace(order_in_transit_qty, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as order_in_transit_qty,
  pack_type,
  makro_unit,
  avg_net_sales_qty,
  net_sales_qty_ytd,
  last_recv_dt,
  last_sold_dt,
  stock_cover_days,
  net_sales_qty_mtd,
  cast(replace(replace(replace(day_1, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_1,
  cast(replace(replace(replace(day_2, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_2,
  cast(replace(replace(replace(day_3, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_3,
  cast(replace(replace(replace(day_4, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_4,
  cast(replace(replace(replace(day_5, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_5,
  cast(replace(replace(replace(day_6, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_6,
  cast(replace(replace(replace(day_7, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_7,
  cast(replace(replace(replace(day_8, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_8,
  cast(replace(replace(replace(day_9, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_9,
  cast(replace(replace(replace(day_10, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_10,
  cast(replace(replace(replace(day_11, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_11,
  cast(replace(replace(replace(day_12, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_12,
  cast(replace(replace(replace(day_13, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_13,
  cast(replace(replace(replace(day_14, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_14,
  cast(replace(replace(replace(day_15, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_15,
  cast(replace(replace(replace(day_16, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_16,
  cast(replace(replace(replace(day_17, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_17,
  cast(replace(replace(replace(day_18, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_18,
  cast(replace(replace(replace(day_19, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_19,
  cast(replace(replace(replace(day_20, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_20,
  cast(replace(replace(replace(day_21, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_21,
  cast(replace(replace(replace(day_22, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_22,
  cast(replace(replace(replace(day_23, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_23,
  cast(replace(replace(replace(day_24, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_24,
  cast(replace(replace(replace(day_25, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_25,
  cast(replace(replace(replace(day_26, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_26,
  cast(replace(replace(replace(day_27, ',', ''), ')', ''), '(', '-') as decimal(10, 4)) as day_27,
  case
    when day_28 is null or day_28 = ''
    then 0
    else cast(replace(replace(replace(day_28, ',', ''), ')', ''), '(', '-') as decimal(10, 4))
  end as day_28,
  case
    when day_29 is null or day_29 = ''
    then 0
    else cast(replace(replace(replace(day_29, ',', ''), ')', ''), '(', '-') as decimal(10, 4))
  end as day_29,
  case
    when day_30 is null or day_30 = ''
    then 0
    else cast(replace(replace(replace(day_30, ',', ''), ')', ''), '(', '-') as decimal(10, 4))
  end as day_30,
  case
    when day_31 is null or day_31 = ''
    then 0
    else cast(replace(replace(replace(day_31, ',', ''), ')', ''), '(', '-') as decimal(10, 4))
  end as day_31,
  file_name,
  crtd_dtm
from  sdl_th_mt_makro)
select * from final
