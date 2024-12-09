with edw_ph_pos_analysis_pos as(

    select * from {{ ref('phledw_integration__edw_ph_pos_analysis') }}
),
edw_ph_pos_analysis_mercury as(

    select * from {{ ref('phledw_integration__edw_ph_pos_analysis_v2') }} where sold_to='107882'
),
edw_ph_pos_analysis as 
(
    select * from  edw_ph_pos_analysis_pos 

    union all

    select * from edw_ph_pos_analysis_mercury
),
hcp_product_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_product_master') }}
),
hcp_store_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_store_master') }}
),

ph_hcp_gmc_brands as (

  select * from {{ ref('phlitg_integration__itg_ph_hcp_gmc_brands') }}

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
    ph_hcp_gmc_brands.GMC_BRAND_NAME as brand,
    store.territory_code_code as territory_code,
    store.DISTRICT_CODE,
    prod.team_code,
    sum(pos.JJ_GTS*store.SALES_ALLOCATED) as POS_GTS
    from edw_ph_pos_analysis pos
     inner join (select distinct code,team_code,GROUP_VARIANT_CODE from hcp_product_master) prod  on (pos.sku=prod.code)
     inner join (select distinct store_code,GROUP_VARIANT_CODE,territory_code_code,customer_code , district_code,SALES_ALLOCATED from  hcp_store_master )store on (pos.CUST_BRNCH_CD=store.store_code and store.GROUP_VARIANT_CODE=prod.GROUP_VARIANT_CODE and pos.cust_cd = store.customer_code)
     left join ph_hcp_gmc_brands ON (ph_hcp_gmc_brands.sap_matl_num=prod.code)
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