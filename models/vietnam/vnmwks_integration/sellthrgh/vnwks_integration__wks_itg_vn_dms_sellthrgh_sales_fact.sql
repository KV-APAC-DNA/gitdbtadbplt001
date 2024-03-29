with sdl_vn_dms_sellthrgh_sales_fact as (
select * from DEV_DNA_LOAD.SNAPOSESDL_RAW.SDL_VN_DMS_SELLTHRGH_SALES_FACT
),
wks_vn_dms_sellthrgh_sales_fact  as (select dstrbtr_id,dstrbtr_type,mapped_spk,doc_number,ref_number,receipt_date,order_type,vat_invoice_number,vat_invoice_note,vat_invoice_date,pon_number,line_ref,product_code,unit,quantity,price,amount,tax_amount,tax_id,tax_rate,"values",line_discount,doc_discount,status,run_id,curr_date as crtd_dttm

from sdl_vn_dms_sellthrgh_sales_fact 

where (dstrbtr_id,mapped_spk,doc_number,product_code) in 

(select dstrbtr_id,mapped_spk,doc_number,product_code

from sdl_vn_dms_sellthrgh_sales_fact

group by dstrbtr_id,mapped_spk,doc_number,product_code

having count(*) > 1)
),
wks_sdl_vn_dms_sellthrgh_sales_fact as (
(select dstrbtr_id,dstrbtr_type,mapped_spk,doc_number,ref_number,receipt_date,order_type,vat_invoice_number,vat_invoice_note,vat_invoice_date,pon_number,line_ref,product_code,unit,quantity,price,amount,tax_amount,tax_id,tax_rate,"values",line_discount,doc_discount,status,run_id,curr_date as crtd_dttm

from sdl_vn_dms_sellthrgh_sales_fact

minus

select dstrbtr_id,dstrbtr_type,mapped_spk,doc_number,ref_number,receipt_date,order_type,vat_invoice_number,vat_invoice_note,vat_invoice_date,pon_number,line_ref,product_code,unit,quantity,price,amount,tax_amount,tax_id,tax_rate,"values",line_discount,doc_discount,status,run_id,crtd_dttm

from wks_vn_dms_sellthrgh_sales_fact)

UNION ALL

select dstrbtr_id,dstrbtr_type,mapped_spk,doc_number,ref_number,receipt_date,order_type,vat_invoice_number,vat_invoice_note,vat_invoice_date,pon_number,line_ref,product_code,unit,quantity,price,amount,tax_amount,tax_id,tax_rate,"values",line_discount,doc_discount,status,run_id,crtd_dttm

from wks_vn_dms_sellthrgh_sales_fact 

where (dstrbtr_id,mapped_spk,doc_number,ref_number) IN 

(select dstrbtr_id,mapped_spk,doc_number,ref_number from wks_vn_dms_sellthrgh_sales_fact  

where status in ('C', 'V') 

group by dstrbtr_id,mapped_spk,doc_number,ref_number

having count(*) = 1)

and status in ('C','V')  --this will give the "values" from (H,c) and (H,V)

UNION ALL

select dstrbtr_id,dstrbtr_type,mapped_spk,doc_number,ref_number,receipt_date,order_type,vat_invoice_number,vat_invoice_note,vat_invoice_date,pon_number,line_ref,product_code,unit,quantity,price,amount,tax_amount,tax_id,tax_rate,"values",line_discount,doc_discount,status,run_id,crtd_dttm

from wks_vn_dms_sellthrgh_sales_fact 

where (dstrbtr_id,mapped_spk,doc_number,ref_number) IN (

select dstrbtr_id,mapped_spk,doc_number,ref_number from wks_vn_dms_sellthrgh_sales_fact where status in ('C', 'V') group by dstrbtr_id,mapped_spk,doc_number,ref_number

having count(*) > 1)

and status = 'V'
)
select * from wks_sdl_vn_dms_sellthrgh_sales_fact