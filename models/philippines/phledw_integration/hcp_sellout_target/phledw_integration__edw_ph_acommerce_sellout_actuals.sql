

with  acommerce_sellout_actuals as (
    select * from {{ ref('phlitg_integration__itg_ph_acommerce_sellout_data') }}
),
hce_product_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_product_master') }}
),
hcp_store_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_store_master') }}
),
hce_customer_master as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_customer_master') }} 
),
 ph_hcp_gmc_brands as (

  select * from {{ ref('phlitg_integration__itg_ph_hcp_gmc_brands') }}

 ),

transformed as 
(
     
   select   
   'HCE_ACTUALS' as data_src,
   hce.jj_mnth_id ,
   hce.jj_year,
    hce.prefix,
    hce.month,
    hce.month_no,
    hce.order_id,
    hce.partner_order_id,
    hce.territory_manager,
    hce.item_sku,
    hce.acommerce_item_sku,
    hce.product_title,
    hce.Brand,
    --ph_hcp_gmc_brands.GMC_BRAND_NAME as Brand,
    hce.Mapping,
    hce.ltp,
    hce.jj_srp,
    hce.quantity,
    hce.customer_email,
    prod.GROUP_VARIANT_CODE,
    
    prod.team_code,
    prod.sap_item_code,
    --store.store_code,
    null as store_code,
    store.territory_code_code  as territory_code,
    store.DISTRICT_CODE,
    cust.name as cust_name,
    cust.code as cust_code,
    cust.direct_manager_code as cust_direct_manager_code,
    hce.gmv as gmv
    from acommerce_sellout_actuals hce
    inner join (select distinct code,GROUP_VARIANT_CODE,team_code,sap_item_code from hce_product_master) prod on (hce.item_sku=prod.code)
    left join (select distinct name,territory_code_code,code,direct_manager_code from hce_customer_master ) cust on (hce.customer_email=cust.name)
    left join (select distinct territory_code_code,DISTRICT_CODE from hcp_store_master) store  on (cust.territory_code_code=store.territory_code_code)
   --  left join ph_hcp_gmc_brands ON (ph_hcp_gmc_brands.sap_matl_num=prod.code)
    group by all
),
final as
(
    select 
    *
    from transformed
)

select * from final 