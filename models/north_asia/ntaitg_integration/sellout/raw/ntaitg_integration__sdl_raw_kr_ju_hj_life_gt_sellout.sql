{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_ju_hj_life_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ju_hj_life_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_ju_hj_life_gt_sellout__null_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  sub_customer_name,
  items,
  ean,
  qty,
  unit,
  total_sales,
  see,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_ju_hj_life_gt_sellout
)
select * from final 