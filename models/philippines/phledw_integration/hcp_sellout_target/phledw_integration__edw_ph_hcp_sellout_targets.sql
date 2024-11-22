with PH_HCP_SELLOUT_TARGET as (
select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_sellout_target') }}

),
HCP_PRODUCT_MASTER as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_product_master') }}
),
HCP_store_MASTER as 
(
    select * from {{ ref('phlitg_integration__itg_ph_mds_hcp_store_master') }}
),
transformed as (
select distinct 
'HCP_SELLOUT_TARGET' as data_src,
substr(SELLOUT_TARGET.year_month,1,4) as jj_year,
SELLOUT_TARGET.year_month as jj_month,
SELLOUT_TARGET.TERRITORY_CODE_CODE as TERRITORY_CODE,
SELLOUT_TARGET.GROUP_VARIANT_CODE,
SELLOUT_TARGET.sellout_target,
prod_master.team_code,
null as store_code,
null as customer_code,
store_master.DISTRICT_CODE

from (select distinct year_month ,TERRITORY_CODE_CODE,GROUP_VARIANT_CODE,sellout_target from  PH_HCP_SELLOUT_TARGET) SELLOUT_TARGET 
  inner join (select distinct GROUP_VARIANT_CODE, team_code from HCP_PRODUCT_MASTER ) prod_master on (SELLOUT_TARGET.GROUP_VARIANT_CODE=
prod_master.GROUP_VARIANT_CODE )

  inner join (select distinct GROUP_VARIANT_CODE,TERRITORY_CODE_CODE,DISTRICT_CODE
  from HCP_store_MASTER ) store_master on (sellout_target.TERRITORY_CODE_CODE=store_master.TERRITORY_CODE_CODE and  SELLOUT_TARGET.GROUP_VARIANT_CODE=
store_master.GROUP_VARIANT_CODE)

),
final as 
(
  select 
    *
  from transformed
)
select * from final