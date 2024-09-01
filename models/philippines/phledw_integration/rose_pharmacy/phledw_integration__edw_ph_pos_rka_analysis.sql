with ph_pos_rka_rose_pharma as 
 (
    select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
 ),
ph_rosepharma_customers as 
(
    select * from phlitg_integration.itg_mds_ph_pos_rosepharma_customers
),
ph_rosepharma_products as 
(
     select * from phlitg_integration.itg_mds_ph_pos_rosepharma_products
),
price_list as 
(
select * from PHLITG_INTEGRATION.ITG_MDS_PH_POS_PRICELIST
),
transformed as 
(
select 
    pos.jj_year,
    pos.jj_month,
    pos.jj_month_id,
    cust.Code,
    cust.brnch_cd,
    cust.brnch_nm,
    split_part(cust.Code,'-',1) as prefix,
    prod.sap_item_cd,
    prod.sap_item_desc,
    POS.qty,
    coalesce(prod.jnj_pc_per_cust_unit,1) as jnj_pc_per_cust_unit,
    price.Lst_Price_Unit as ListPriceUnit,
    (pos.qty/prod.jnj_pc_per_cust_unit) as pos_qty,
    (pos_qty*ListPriceUnit) as pos_gts

    from ph_pos_rka_rose_pharma as pos
    join ph_rosepharma_products prod on (pos.sku=prod.item_cd)
    join ph_rosepharma_customers cust on (pos.branch_code=cust.brnch_cd)
    join price_list price on (prod.sap_item_cd=price.item_cd and prod.mnth_id=price.jj_mnth_id) 
  ),
final as 
(
select 
*
from transformed

)
select * from final 