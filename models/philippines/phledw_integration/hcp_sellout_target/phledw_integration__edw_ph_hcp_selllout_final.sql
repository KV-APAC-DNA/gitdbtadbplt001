with edw_ph_pos_sellout_actuals as 
(

    select * from {{ ref('phledw_integration__edw_ph_pos_sellout_actuals') }}
),
hcp_sellout_targets  as 
  (
    select * from {{ ref('phledw_integration__edw_ph_hcp_sellout_targets') }}
  ),

acommerce_sellout_actuals as
(
 select * from {{ ref('phledw_integration__edw_ph_acommerce_sellout_actuals') }}
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
    store_code ,
    sku,
    GROUP_VARIANT_CODE,
    brand,
    territory_code,
    team_code,
    DISTRICT_CODE,
    null as sellout_target,
     POS_GTS as sell_out,
     null as CUSTOMER_COUNT_TARGET,
     null as cust_code,
     null as cust_name ,
     null as cust_direct_manager_code
    from edw_ph_pos_sellout_actuals
  
  union all
 
   select  
 data_src  :: varchar(50) as data_src,
 jj_month as jj_mnth_id,
  jj_year,
  store_code,
  null as sku,
GROUP_VARIANT_CODE,
null as brand,
territory_code,
team_code,
DISTRICT_CODE,
sellout_target,
null as sell_out,
null as CUSTOMER_COUNT_TARGET,
null as cust_code,
     null as cust_name ,
     null as cust_direct_manager_code
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
    brand,
    territory_code,
    team_code,
    DISTRICT_CODE,
    null as sellout_target,
     sum(gmv) as sellout,
     null as CUSTOMER_COUNT_TARGET,
     cust_code as cust_code,
     cust_name  as cust_name ,
     cust_direct_manager_code as cust_direct_manager_code
    from 
    acommerce_sellout_actuals
   group by all

   union all 

select 
    data_src :: varchar(50) as data_src,
    jj_mnth_id ,
    JJ_YEAR,
    null as store_code,
    sku,
    GROUP_VARIANT_CODE,
    null as brand,
    territory_code,
    team_code,
    DISTRICT_CODE,
    sellout_target ,
     null as sellout,
     CUSTOMER_COUNT_TARGET as CUSTOMER_COUNT_TARGET,
     null as cust_code,
     null as cust_name ,
     null as cust_direct_manager_code
    from 
    edw_ph_hce_sellout_targets
   group by all
),

final as 
(
    select 
    data_src,
    JJ_MNTH_ID :: integer as JJ_MNTH_ID ,
    JJ_YEAR :: integer as JJ_YEAR,
    store_code :: integer as store_code,
    sku :: varchar(100) as sku,
    GROUP_VARIANT_CODE:: varchar(100) as GROUP_VARIANT_CODE,
    brand :: varchar(50) as brand,
    territory_code :: varchar(100) as territory_code,
    team_code :: varchar(100) as team_code,
    DISTRICT_CODE :: varchar(100) as DISTRICT_CODE,
    sellout_target:: decimal(38,6) as sellout_target,
    sell_out :: decimal(38,6) as sell_out,
    CUSTOMER_COUNT_TARGET :: integer as CUSTOMER_COUNT_TARGET,
    cust_code :: varchar(200) as cust_code,
     cust_name :: varchar(200) as cust_name ,
     cust_direct_manager_code :: varchar(200) as cust_direct_manager_code
    from transformed
)
select * from final