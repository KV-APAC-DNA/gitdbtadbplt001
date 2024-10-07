with  acommerce_sellout_target as (
    select * from {{ ref('phledw_integration__edw_ph_acommerce_sellout_target') }}
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
transformed as 
(
    select 

    from acommerce_sellout_target
    inner join hce_product_master
    inner hcp_store_master
),
final as
(
    select 
    *

    from transformed
)

select * from final 