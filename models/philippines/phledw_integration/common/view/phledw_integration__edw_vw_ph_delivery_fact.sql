with edw_delivery_fact as (
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_DELIVERY_FACT
),
final as (
SELECT 
  DISTINCT CASE WHEN (
    (edw_delivery_fact.sls_org):: text = '2300' :: text
  ) THEN 'PH' :: text  ELSE NULL :: text END AS cntry_key, 
  CASE WHEN (
    (edw_delivery_fact.sls_org):: text = '2300' :: text
  ) THEN 'PHILIPPINES' :: text  ELSE NULL :: text END AS cntry_nm, 
  ltrim(
    (edw_delivery_fact.deliv_num):: text, 
    '0' :: text
  ) AS deliv_num, 
  ltrim(
    (edw_delivery_fact.deliv_item):: text, 
    '0' :: text
  ) AS deliv_item, 
  edw_delivery_fact.ship_dt AS delvry_dt, 
  edw_delivery_fact.zactdldte AS act_delvry_dt, 
  edw_delivery_fact.created_on AS doc_creation_dt, 
  ltrim(
    (edw_delivery_fact.sold_to):: text, 
    '0' :: text
  ) AS sold_to, 
  ltrim(
    (edw_delivery_fact.material):: text, 
    '0' :: text
  ) AS matl_num, 
  edw_delivery_fact.item_categ AS item_catgy, 
  edw_delivery_fact.eanupc AS ean_num, 
  edw_delivery_fact.sls_org, 
  ltrim(
    (edw_delivery_fact.ship_to):: text, 
    '0' :: text
  ) AS ship_to, 
  edw_delivery_fact.bill_block AS billing_block, 
  edw_delivery_fact.whse_num, 
  edw_delivery_fact.base_uom, 
  edw_delivery_fact.zdelqtybu AS delvrd_qty_pc, 
  edw_delivery_fact.act_dl_qty AS act_delvry_qty 
FROM 
  edw_delivery_fact 
WHERE 
  
            (edw_delivery_fact.sls_org):: text = '2300' :: text
          
  )
  select * from final
