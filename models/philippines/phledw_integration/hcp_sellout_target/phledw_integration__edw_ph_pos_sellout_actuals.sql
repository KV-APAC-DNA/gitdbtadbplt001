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
rose_pharma_sellout_data as 
(
  select 1 -- place holder for rose pharma data 
),
POS as 
(
  select 
   'POS_SELLOUT_ACTUALS' as data_src,
    pos.JJ_MNTH_ID,
    pos.JJ_YEAR,
    pos.ITEM_CD,
    pos.CUST_BRNCH_CD as store_code,
    pos.sku,
    store.GROUP_VARIANT_CODE,
    store.territory_code_code as territory_code,
    store.DISTRICT_CODE,
    prod.team_code,
    sum(pos.JJ_GTS) as POS_GTS
    from edw_ph_pos_analysis pos
     inner join (select distinct code,team_code,GROUP_VARIANT_CODE from hcp_product_master) prod  on (pos.sku=prod.code)
     inner join (select distinct store_code,GROUP_VARIANT_CODE,territory_code_code,customer_code , district_code from  hcp_store_master )store on (pos.CUST_BRNCH_CD=store.store_code and store.GROUP_VARIANT_CODE=prod.GROUP_VARIANT_CODE and pos.cust_cd = store.customer_code)
     group by all
),
RKA as 
(
    select 1 
),
transformed as 
(
    select * from POS
  
),
final as 
(
    select 
    * 
    from 
    --transformed 
    pos
)
select * from final