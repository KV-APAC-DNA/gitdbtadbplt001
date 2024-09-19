{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_da_in_sang_sa_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_da_in_sang_sa_gt_sellout') }}
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  kejo,
  sub_customer_name,
  item,
  gyu,
  bigo,
  qty,
  price,
  gum,
  vat,
  gumvat,
  ean,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_da_in_sang_sa_gt_sellout
)
select * from final 