{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_hyundai_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_hyundai_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_hyundai_gt_sellout__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_hyundai_gt_sellout__lookup_test') }}
    )
),
final as (
SELECT dstr_nm,
  ims_txn_dt,
  burial_name,
  ean,
  article_name,
  normal_sales,
  qty,
  sales,
  unit_price,
  dc_rate,
  margin_rate,
  margin_normal,
  margin_sold,
  unit_prc_pres,
  sub_customer_name,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_hyundai_gt_sellout

)
select * from final 