with
wk16_ua02_directmail_main_channel_bddm as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.WK16_UA02_DIRECTMAIL_MAIN_CHANNEL_BDDM
),
wk15_ua02_directmail_stage as (
select * from {{ ref('jpndcledw_integration__wk15_ua02_directmail_stage') }}
),
wk12_ua02_directmail_other_adv_flg as (
select * from {{ ref('jpndcledw_integration__wk12_ua02_directmail_other_adv_flg') }}
),
wk11_ua02_directmail_register_card_flg as (
select * from {{ ref('jpndcledw_integration__wk11_ua02_directmail_register_card_flg') }}
),
ua01_base_cim01kokya_v as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.ua01_base_cim01kokya_v
),
wk14_ua02_directmail_familysale_class as (
select * from {{ ref('jpndcledw_integration__wk14_ua02_directmail_familysale_class') }}
),
wk13_ua02_directmail_main_store as (
select * from {{ ref('jpndcledw_integration__wk13_ua02_directmail_main_store') }}
),
wk10_ua02_directmail_biken_flg as (
select * from {{ ref('jpndcledw_integration__wk10_ua02_directmail_biken_flg') }}
),
transformed as (
SELECT 
  DISTINCT ckv.kokyano, 
  a.biken_flg, 
  b.register_card_flg, 
  c.other_adv_flg, 
  d.main_store, 
  e.familysale_class, 
  g.main_channel_bddm, 
  f.promo_stage, 
  f.next_promo_stage_amt, 
  f.next_promo_stage_point, 
  f.point_tobe_granted, 
  f.stage, 
  f.stage_monthly, 
  f.totalprc_this_year 
FROM 
  ua01_base_cim01kokya_v ckv 
  LEFT JOIN wk10_ua02_directmail_biken_flg a ON ckv.kokyano = a.customer_no 
  LEFT JOIN wk11_ua02_directmail_register_card_flg b ON ckv.kokyano = b.customer_no 
  LEFT JOIN wk12_ua02_directmail_other_adv_flg c ON ckv.kokyano = c.customer_no 
  LEFT JOIN wk13_ua02_directmail_main_store d ON ckv.kokyano = d.customer_no 
  LEFT JOIN wk14_ua02_directmail_familysale_class e ON ckv.kokyano = e.customer_no 
  LEFT JOIN wk15_ua02_directmail_stage f ON ckv.kokyano = f.customer_no 
  LEFT JOIN wk16_ua02_directmail_main_channel_bddm g ON ckv.kokyano = g.customer_no
),
final as (
select
kokyano::varchar(60) as kokyano,
biken_flg::number(18,0) as biken_flg,
register_card_flg::number(18,0) as register_card_flg,
other_adv_flg::number(18,0) as other_adv_flg,
main_store::varchar(60) as main_store,
familysale_class::number(18,0) as familysale_class,
main_channel_bddm::varchar(10) as main_channel_bddm,
promo_stage::varchar(18) as promo_stage,
next_promo_stage_amt::number(18,0) as next_promo_stage_amt,
next_promo_stage_point::number(18,0) as next_promo_stage_point,
point_tobe_granted::number(18,0) as point_tobe_granted,
stage::varchar(18) as stage,
stage_monthly::varchar(18) as stage_monthly,
totalprc_this_year::number(38,0) as totalprc_this_year
from transformed
)
select * from final
