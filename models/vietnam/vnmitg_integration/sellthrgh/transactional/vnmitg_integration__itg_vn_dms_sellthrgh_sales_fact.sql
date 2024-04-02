{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["dstrbtr_id","mapped_spk","doc_number","product_code"]
    )
}}

with wks_itg_vn_dms_sellthrgh_sales_fact as (
    select * from {{ ref('vnwks_integration__wks_itg_vn_dms_sellthrgh_sales_fact') }}
),
transformed as (
select 
  dstrbtr_id, 
  dstrbtr_type, 
  mapped_spk, 
  doc_number, 
  ref_number, 
  to_date(receipt_date, 'MM/DD/YYYY HH12:MI:SS AM') as receipt_date, 
  trim(order_type) as order_type, 
  vat_invoice_number, 
  vat_invoice_note, 
  to_date(vat_invoice_date, 'MM/DD/YYYY HH12:MI:SS AM') as vat_invoice_date, 
  pon_number, 
  line_ref, 
  product_code, 
  unit, 
  cast(quantity as int) as quantity, 
  cast(
    price as numeric(15, 4)
  ) as price, 
  cast(
    amount as numeric(15, 4)
  ) as amount, 
  cast(
    tax_amount as numeric(15, 4)
  ) as tax_amount, 
  tax_id, 
  cast(tax_rate as numeric) as tax_rate, 
  cast(
    "values" 
      as numeric
  ) as "values", 
  cast(
    line_discount as numeric(15, 4)
  ) as line_discount, 
  cast(
    doc_discount as numeric(15, 4)
  ) as doc_discount, 
  status, 
  current_timestamp() as crtd_dttm, 
  current_timestamp() as updt_dttm, 
  run_id 
from 
wks_itg_vn_dms_sellthrgh_sales_fact),
final as (
select
dstrbtr_id::varchar(30) as dstrbtr_id,
dstrbtr_type::varchar(30) as dstrbtr_type,
mapped_spk::varchar(30) as mapped_spk,
doc_number::varchar(30) as doc_number,
ref_number::varchar(30) as ref_number,
receipt_date::date as receipt_date,
order_type::varchar(2) as order_type,
vat_invoice_number::varchar(30) as vat_invoice_number,
vat_invoice_note::varchar(30) as vat_invoice_note,
vat_invoice_date::date as vat_invoice_date,
pon_number::varchar(40) as pon_number,
line_ref::varchar(10) as line_ref,
product_code::varchar(50) as product_code,
unit::varchar(10) as unit,
quantity::number(14,2) as quantity,
price::number(15,4) as price,
amount::number(15,4) as amount,
tax_amount::number(15,4) as tax_amount,
tax_id::varchar(10) as tax_id,
tax_rate::number(15,4) as tax_rate,
"values":: number(15,4) as "values",
line_discount::number(15,4) as line_discount,
doc_discount::number(15,4) as doc_discount,
status::varchar(1) as status,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
run_id::number(14,0) as run_id
from transformed
)

select * from final 
