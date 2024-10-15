with edw_ph_pos_sellout_target as 
(

    select * from {{ ref('phledw_integration__edw_ph_pos_sellout_targets') }}
),
hcp_sellout_targets  as 
  (
    select * from {{ ref('phledw_integration__edw_ph_hcp_sellout_targets') }}
  ),

acommerce_sellout_target as
(
 select * from {{ ref('phledw_integration__edw_ph_acommerce_sellout_target') }}
),
edw_ph_hce_sellout_targets as 
 (
  select * from {{ ref('phledw_integration__edw_ph_hce_sellout_targets') }}
 ),
transformed as 
(
    select 
    data_src :: varchar(50) as data_src,
    JJ_MNTH_ID ,
    JJ_YEAR,
    store_code,
    sku,
    GROUP_VARIANT_CODE,
    territory_code_code,
    team_code,
    DISTRICT_CODE,
    null as sellout_target,
     POS_GTS as sell_out
    from edw_ph_pos_sellout_target
  
  union all
 
   select  
 data_src  :: varchar(50) as data_src,
 jj_month as jj_mnth_id,
  jj_year,
  store_code,
  null as sku,
GROUP_VARIANT_CODE,
TERRITORY_CODE_CODE,
team_code,
DISTRICT_CODE,
sellout_target,
null as sell_out
  from 
  hcp_sellout_targets

union all 

select 
    data_src :: varchar(50) as data_src,
    jj_mnth_id ,
    JJ_YEAR,
    store_code,
    item_sku as sku,
    GROUP_VARIANT_CODE,
    territory_code_code,
    team_code,
    DISTRICT_CODE,
    null as sellout_target,
     sum(gmv) as sellout
    from 
    acommerce_sellout_target
   group by all

),

final as 
(
    select 
    *
    from transformed
)
select * from final