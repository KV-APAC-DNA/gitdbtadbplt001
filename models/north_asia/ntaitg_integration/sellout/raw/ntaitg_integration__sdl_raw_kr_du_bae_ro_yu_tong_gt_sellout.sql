{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_du_bae_ro_yu_tong_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_du_bae_ro_yu_tong_gt_sellout') }}
),
final as (
SELECT dstr_nm,
  s_no,
  sub_customer_name,
  ims_txn_dt,
  ean,
  neck_name,
  standard,
  unit,
  qty,
  unit_price,
  total,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_du_bae_ro_yu_tong_gt_sellout
)
select * from final 