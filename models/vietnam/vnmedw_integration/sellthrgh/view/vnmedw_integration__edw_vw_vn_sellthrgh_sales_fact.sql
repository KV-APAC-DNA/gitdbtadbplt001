with itg_vn_dms_sellthrgh_sales_fact as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_sellthrgh_sales_fact') }}
),
itg_vn_dms_distributor_dim_rnk as (
select *,Row_number() over (partition by dstrbtr_id order by crtd_dttm desc) as rnk  
from {{ ref('vnmitg_integration__itg_vn_dms_distributor_dim') }}  
),
itg_vn_dms_distributor_dim as (
select * from itg_vn_dms_distributor_dim_rnk where rnk=1
),
itg_vn_dms_product_dim as (
select * from {{ ref('vnmitg_integration__itg_vn_dms_product_dim') }}
),
final as (
SELECT distinct
  'VN' AS cntry_cd, 
  'Vietnam' AS cntry_nm, 
  sellthrgh.dstrbtr_id, 
  dstrbtr.dstrbtr_name, 
  sellthrgh.mapped_spk, 
  sellthrgh.product_code AS matl_num, 
  prod_dim.productcodesap AS sap_matl_num, 
  sellthrgh.vat_invoice_date AS bill_date, 
  sellthrgh.doc_number AS bill_doc, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('TR' :: character varying):: text
  ) THEN sellthrgh.quantity ELSE (0):: numeric END AS sls_qty, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('RT' :: character varying):: text
  ) THEN sellthrgh.quantity ELSE (0):: numeric END AS ret_qty, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('TR' :: character varying):: text
  ) THEN sellthrgh.quantity ELSE (0):: numeric END AS sls_qty_pc, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('RT' :: character varying):: text
  ) THEN sellthrgh.quantity ELSE (0):: numeric END AS ret_qty_pc, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('TR' :: character varying):: text
  ) THEN sellthrgh.amount ELSE (
    (0):: numeric
  ):: numeric(18, 0) END AS grs_trd_sls, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('RT' :: character varying):: text
  ) THEN sellthrgh.amount ELSE (
    (0):: numeric
  ):: numeric(18, 0) END AS ret_val, 
  sellthrgh.line_discount AS trd_discnt_item_lvl, 
  sellthrgh.doc_discount AS trd_discnt_bill_lvl, 
  (
    sellthrgh.amount - (
      sellthrgh.line_discount + sellthrgh.doc_discount
    )
  ) AS net_trd_sls, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('TR' :: character varying):: text
  ) THEN sellthrgh.amount ELSE (
    (0):: numeric
  ):: numeric(18, 0) END AS jj_grs_trd_sls, 
  CASE WHEN (
    (sellthrgh.order_type):: text = ('RT' :: character varying):: text
  ) THEN sellthrgh.amount ELSE (
    (0):: numeric
  ):: numeric(18, 0) END AS jj_ret_val, 
  NULL :: integer AS jj_trd_sls, 
  (
    CASE WHEN (
      (
        (sellthrgh.order_type):: text = ('TR' :: character varying):: text
      ) 
      OR (
        (sellthrgh.order_type IS NULL) 
        AND ('TR' IS NULL)
      )
    ) THEN sellthrgh.amount ELSE (- sellthrgh.amount) END - (
      sellthrgh.line_discount + sellthrgh.doc_discount
    )
  ) AS jj_net_trd_sls 
FROM 
  itg_vn_dms_sellthrgh_sales_fact sellthrgh, 
  itg_vn_dms_distributor_dim dstrbtr, 
  itg_vn_dms_product_dim prod_dim 
WHERE 
  
        trim((sellthrgh.dstrbtr_id)):: text = trim((dstrbtr.dstrbtr_id)):: text
      
      AND 
        (prod_dim.product_code):: text = (sellthrgh.product_code):: text
      
    
    AND (
      (sellthrgh.status):: text <> ('V' :: character varying):: text
    )
  
)
select * from final 