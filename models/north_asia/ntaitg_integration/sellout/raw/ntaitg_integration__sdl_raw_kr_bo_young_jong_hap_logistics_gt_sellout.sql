{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_bo_young_jong_hap_logistics_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_bo_young_jong_hap_logistics_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_bo_young_jong_hap_logistics_gt_sellout__null_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  origin_code,
  sub_customer_name,
  ean,
  booklet_code,
  trade_name,
  standard,
  unit,
  qty,
  unit_price,
  cust_cd,
  current_timestamp() as crtd_dttm,
  file_name::varchar(255) as file_name
FROM sdl_kr_bo_young_jong_hap_logistics_gt_sellout
)
select * from final 