{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_jungseok_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_jungseok_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_jungseok_gt_sellout__null_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  s_no,
  sub_customer_name,
  ean,
  neck_name,
  the_rules,
  master_file_name,
  qty,
  amount,
  tax,
  total,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_jungseok_gt_sellout
)
select * from final 