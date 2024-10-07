with hce_portal_targets as(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_portal_targets') }}
),
hce_product_master as 
 (
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_product_master') }}
 ),
 transformed as 
 (

    select 
     'HCE_TARGETS' as data_src,
     target.year_month,
     target.territory_code_code,
     target.GROUP_VARIANT_CODE,
     target.gmv_target,
     prod.team_code,
     prod.sap_item_code,
     prod.code
    from hce_portal_targets target
    inner join hce_product_master prod on (target.GROUP_VARIANT_CODE=prod.GROUP_VARIANT_CODE)
 ),
 final  as 
 (
    select
    * 
    from 
    transformed
 )
 
 select * from final

