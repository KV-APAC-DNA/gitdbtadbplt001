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
    select 1 
),
hce_customer_master as 
(
    select 1
)
transformed as 
(
     
   select 
   'acommerce' as data_source,
   hce.mnth_id,
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
    prod.GROUP_VARIANT_CODE,
    prod.team_code,
    prod.sap_item_code

    from acommerce_sellout_target hce
    inner join hce_product_master prod on (hce.item_sku=prod.code)
    -- inner join hce_customer_master 
    --inner hcp_store_master store 
),
final as
(
    select 
    *

    from transformed
)

select * from final 