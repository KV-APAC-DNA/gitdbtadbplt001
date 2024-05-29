{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_hyundai_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_hyundai_gt_sellout') }}
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
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final 