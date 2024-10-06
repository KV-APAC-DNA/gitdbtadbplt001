with edw_ph_pos_analysis as(

    select * from {{ ref('phledw_integration__edw_ph_pos_analysis') }}
),
hcp_product_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_product_master') }}
),
hcp_store_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_store_master') }}
),
transformed as 
(
  select 
    pos.JJ_MNTH_ID,
    pos.sku,
    pos.ITEM_CD,
    pos.POS_GTS,
    pos.POS_NTS,
    pos.POS_QTY,
    pos.CUST_BRNCH_CD,
    store.GROUP_VARIANT_CODE,
    store.territory_code_code
    from edw_ph_pos_analysis pos
     inner join hcp_product_master prod  on (pos.sku=prod.code)
     inner join hcp_store_master store on (pos.CUST_BRNCH_CD=store.store_code and store.GROUP_VARIANT_CODE=prod.GROUP_VARIANT_CODE)
),
final as 
(
    select 
    * 
    from 
    transformed
)
select * from final