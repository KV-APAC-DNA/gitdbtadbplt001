with hce_portal_targets as(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_portal_targets') }}
),
hce_product_master as 
 (
    select * from {{ ref('phlitg_integration__itg_ph_mds_hce_product_master') }}
 ),
 edw_vw_os_time_dim as 
(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
 transformed as 
 (

    select distinct
     'HCE_TARGETS' as data_src,
     target.year_month as jj_mnth_id,
     cal.cal_year as jj_year,
     target.territory_code_code as territory_code,
     target.GROUP_VARIANT_CODE,
     target.gmv_target as sellout_target,
     target.CUSTOMER_COUNT_TARGET as CUSTOMER_COUNT_TARGET,
     target.district_code,
     prod.team_code,
     null as sap_item_code,
     null  as sku 
    from hce_portal_targets target
    inner join (select distinct group_variant_code,team_code,sap_item_code,code from  hce_product_master) prod on (target.GROUP_VARIANT_CODE=prod.GROUP_VARIANT_CODE)
    inner join (select distinct mnth_long,mnth_no,mnth_id,cal_year from edw_vw_os_time_dim ) cal on (cal.mnth_id=target.year_month)
 ),
 final  as 
 (
    select
    * 
    from 
    transformed
 )
 
 select * from final

