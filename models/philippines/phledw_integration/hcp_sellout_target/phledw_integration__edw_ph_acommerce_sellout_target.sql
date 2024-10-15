with  acommerce_sellout_target as (
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

transformed as 
(
     
   select 
   'ACOMMERCE' as data_src,
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
    hce.Mapping,
    hce.ltp,
    hce.jj_srp,
    hce.quantity,
    hce.gmv,
    hce.customer_email,
    prod.GROUP_VARIANT_CODE,
    prod.team_code,
    prod.sap_item_code,
    store.store_code,
    store.territory_code_code,
    store.DISTRICT_CODE
    from acommerce_sellout_target hce
    inner join hce_product_master prod on (hce.item_sku=prod.code)
    inner join hce_customer_master  cust on (hce.customer_email=cust.name)
    inner join hcp_store_master store  on (cust.territory_code_code=store.territory_code_code)
),
final as
(
    select 
    *
    from transformed
)

select * from final 