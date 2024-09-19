{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_da_in_sang_sa_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_da_in_sang_sa_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_gt_sellout_da_in_sang_sa__null_test') }}
    )
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
  current_timestamp() as crtd_dttm,
  file_name::varchar(255) as file_name
FROM sdl_kr_da_in_sang_sa_gt_sellout
)
select * from final 