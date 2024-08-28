with ph_pos_pricelist as(
    select * from {{ ref('phlitg_integration__itg_mds_ph_pos_pricelist') }}
),
PH_RKA_Customers as(
    select * from {{ source('phlitg_integration'.'ph_rka_Customers') }}
),
ph_pos_product_dim as 
(
    select * from {{source('phlitg_integration'.'itg_ph_pos_product_dim')}}
),
mds_ph_ref_rka_master as 
 (
    select * from {{ ref('phlitg_integration__itg_mds_ph_ref_rka_master') }}
 ),
ph_pos_rka_rose_pharma as 
 (
select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
 ),
transformed as 
(
select 
  '20'||split_part(pos.month,'/',1) as year jj_year,
   split_part(pos.month,'/',2) as  jj_month,
   '20'||replace(pos.month,'/','') as jj_month_id,
    pos.Code,
    brnch_cd,
    brnch_nm,
    prefix,
    Code,
    sap_item_cd,
    JJ_item_Description,
    UOM,
    jnj_pc_per_cust_unit,
    ListPriceUnit,
    pos_qty,
    (pos_qty*ListPriceUnit) as pos_gts

    from ph_pos_rka_rose_pharma as pos

    join ph_pos_product_dim as prod on (pos.sku=prod.item_cd)
    join PH_RKA_Customers as cust on (prod.code=cust.code)


),
final as 
(
select 
*
from transformed

)
select * from final 