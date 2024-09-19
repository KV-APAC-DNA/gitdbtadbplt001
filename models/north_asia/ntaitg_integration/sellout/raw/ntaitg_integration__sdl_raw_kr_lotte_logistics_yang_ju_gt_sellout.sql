{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_lotte_logistics_yang_ju_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_lotte_logistics_yang_ju_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_lotte_logistics_yang_ju_gt_sellout__null_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  heavy_classification,
  sub_classification,
  code,
  ean,
  product_name,
  account_name,
  ldu,
  supply_division,
  sls_qty,
  sls_amt,
  sales_priority,
  sales_stores,
  sales_rate,
  cust_cd,
  current_timestamp() as crtd_dttm,
  file_name::varchar(255) as file_name
FROM sdl_kr_lotte_logistics_yang_ju_gt_sellout
)
select * from final 