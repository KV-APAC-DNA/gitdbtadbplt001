{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_jungseok_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_jungseok_gt_sellout') }}
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
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final 