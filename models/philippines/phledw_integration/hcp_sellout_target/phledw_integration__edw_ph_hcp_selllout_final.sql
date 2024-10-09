with edw_ph_pos_sellout_target as 
(

    select * from {{ ref('phledw_integration__edw_ph_pos_sellout_targets') }}
),
hcp_sellout_targets  as 
  (
    select * from {{ ref('phledw_integration__edw_ph_hcp_sellout_targets') }}
  ),
transformed as 
(
    select 
    data_src as varchar(50),
    JJ_MNTH_ID ,
    JJ_YEAR,
    store_code,
    sku,
    GROUP_VARIANT_CODE,
    territory_code_code,
    team_code,
    DISTRICT_CODE,
    null as sellout_target,
     POS_GTS
    from edw_ph_pos_sellout_target;
  
  union all
 
   select  
 data_src ,
 jj_month as jj_mnth_id,
  jj_year,
  store_code,
  null as sku,
GROUP_VARIANT_CODE,
TERRITORY_CODE_CODE,
team_code,
DISTRICT_CODE,
sellout_target,
null as POS_GTS,
  from hcp_sellout_targets;


),
final as 
(
    select 
    *
    from transformed
)
select * from final