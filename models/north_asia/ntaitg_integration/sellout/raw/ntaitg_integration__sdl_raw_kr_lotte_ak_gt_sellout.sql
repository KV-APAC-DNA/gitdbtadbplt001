{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_lotte_ak_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_lotte_ak_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_lotte_ak_gt_sellout__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_lotte_ak_gt_sellout__lookup_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  ean,
  article_name,
  normal_sales,
  unit_price,
  dc_rate,
  qty,
  event_sales,
  margin_27,
  margin_22,
  receipt,
  vnd_margin_22,
  unit_prc_diff,
  fin_unit_prc,
  sub_customer_name,
  cust_cd,
  current_timestamp() as crtd_dttm,
  file_name::varchar(255) as file_name
FROM sdl_kr_lotte_ak_gt_sellout
)
select * from final 