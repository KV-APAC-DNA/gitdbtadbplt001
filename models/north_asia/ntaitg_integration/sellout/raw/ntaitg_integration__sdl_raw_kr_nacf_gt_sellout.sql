{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with sdl_kr_nacf_gt_sellout as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_nacf_gt_sellout') }}
),
final as (
SELECT dstr_nm,
  customer_code,
  account_name,
  ims_txn_dt,
  inspection_date,
  scheduled_date_of_payment,
  scheduled_delivery_number,
  innovation_design,
  pb,
  sub_customer_code,
  sub_customer_name,
  economic_integration,
  business_name,
  supply_type,
  system_contract_classification,
  document_serial_number,
  ean,
  year_of_production,
  trade_name,
  product_standard_name,
  tax_code_name,
  wearing_weight,
  quantity_of_goods,
  unit_price,
  sales_qty,
  supply_amount,
  purchase_tax,
  purchase_amount,
  current_timestamp() as crtd_dttm
FROM sdl_kr_nacf_gt_sellout
)
select * from final 