with ph_pos_rka_rose_pharma as 
 (
select * from {{ ref('phlitg_integration__itg_ph_pos_rka_rose_pharma') }}
 ),
rosepharma_customers as 
(
 select * from {{source('phlitg_integration'.'itg_mds_ph_pos_rosepharma_customers')}}
),
rosepharma_products as 
(
 select * from {{source('phlitg_integration'.'itg_mds_ph_pos_rosepharma_products')}}
),
transformed as 
(
select 
    jj_year,
    jj_month,
    jj_month_id,
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

  


),
final as 
(
select 
*
from transformed

)
select * from final 