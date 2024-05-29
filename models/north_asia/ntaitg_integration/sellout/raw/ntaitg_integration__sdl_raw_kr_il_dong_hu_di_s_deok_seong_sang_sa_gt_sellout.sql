{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout') }}
),
final as (
SELECT dstr_nm,
  ccode,
  sub_customer_name,
  gcode,
  on_site_name,
  year,
  ims_txn_dt,
  transaction_number,
  product_classification,
  product_code,
  management_code,
  ean,
  prize_name,
  classification,
  rules,
  color,
  delivery_date,
  deliver,
  factory_status,
  number_of_goods,
  BOX,
  one_piece,
  qty,
  weight,
  list,
  unit_price_rate,
  box_danga,
  unit_price,
  cust_cd,
  current_timestamp() as crtd_dttm
FROM sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final 