{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_dongbu_lsd_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_dongbu_lsd_gt_sellout') }}
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  number,
  sub_customer_name,
  total_amount,
  total_room_amount,
  ean,
  product,
  unit,
  qty,
  unit_price,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_dongbu_lsd_gt_sellout

)
select * from final 