with ua02_behavior02 as (
select * from {{ ref('jpndcledw_integration__ua02_behavior02') }}
),
ua01_base_cim01kokya_v as (
select * from {{ ref('jpndcledw_integration__ua01_base_cim01kokya_v') }}
),
ua02_directmail as (
select * from {{ ref('jpndcledw_integration__ua02_directmail') }}
),
ua02_behavior01 as (
select * from {{ ref('jpndcledw_integration__ua02_behavior01') }}
),
transformed as (
SELECT
DISTINCT 
ckv.kokyano,
ckv.zipcode,
ckv.todofukenname,
ckv.seibetukbn,
ckv.birthday,
ckv.birthday_md,
ckv.kokyakbn,
ckv.dmtenpoflg,
ckv.dmtsuhanflg,
ckv.teltenpoflg,
ckv.teltsuhanflg,
ckv.firstmediacode,
ckv.taikai_flg,
ckv.webno,
ub1.col1,
ub1.col2,
ub1.col3,
ub1.col4,
ub1.col5,
ub1.col6,
ub1.col7,
ub1.col8,
ub1.col9,
ub1.col10,
ub1.col11,
ub1.col12,
ub1.col13,
ub1.col14,
ub1.col15,
ub1.col16,
ub1.col17,
ub1.col18,
ub1.col19,
ub1.col20,
ub1.col21,
ub1.col22,
ub1.col23,
ub1.col24,
ub1.col25,
ub1.col26,
ub1.col27,
ub1.col28,
ub1.col29,
ub1.col30,
ub1.col31,
ub1.col32,
ub1.col33,
ub1.col34,
ub1.col35,
ub1.col36,
ub1.col37,
ub1.col38,
ub1.col39,
ub1.col40,
ub1.col41,
ub1.col42,
ub1.col43,
ub1.col44,
ub1.col45,
ub1.col46,
ub1.col47,
ub1.col48,
ub1.col49,
ub1.col50,
ub1.col51,
ub1.col52,
ub1.col53,
ub1.col54,
ub1.col55,
ub1.col56,
ub1.col57,
ub1.col58,
ub1.col59,
ub1.col60,
ub1.col61,
ub1.col62,
ub1.col63,
ub1.col64,
ub1.col65,
ub1.col66,
ub1.col67,
ub1.col68,
ub1.col69,
ub1.col70,
ub1.col71,
ub1.col72,
ub1.col73,
ub1.col74,
ub1.col75,
ub1.col76,
ub1.col77,
ub1.col78,
ub1.col79,
ub1.col80,
ub1.col81,
ub1.col82,
ub1.col83,
ub1.col84,
ub1.col85,
ub1.col86,
ub1.col87,
ub1.col88,
ub1.col89,
ub1.col90,
ub1.col91,
ub1.col92,
ub1.col93,
ub1.col94,
ub1.col95,
ub1.col96,
ub1.col97,
ub1.col98,
ub1.col99,
ub1.col100,
ub1.col101,
ub1.col102,
ub1.col103,
ub1.col104,
ub1.col105,
ub1.col106,
ub1.col107,
ub1.col108,
ub1.col109,
ub1.col110,
ub1.col111,
ub1.col112,
ub1.col113,
ub1.col114,
ub1.col115,
ub1.col116,
ub1.col117,
ub1.col118,
ub1.col119,
ub1.col120,
ub1.col121,
ub1.col122,
ub1.col123,
ub1.col124,
ub1.col125,
ub1.col126,
ub1.col127,
ub1.col128,
ub1.col129,
ub1.col130,
ub1.col131,
ub1.col132,
ub1.col133,
ub1.col134,
ub1.col135,
ub1.col136,
ub1.col137,
ub1.col138,
ub1.col139,
ub1.col140,
ub1.col141,
ub1.col142,
ub1.col143,
ub1.col144,
ub1.col145,
ckv.register_dt,
udm.biken_flg,
udm.register_card_flg,
udm.other_adv_flg,
udm.main_store,
udm.familysale_class,
udm.main_channel_bddm,
udm.promo_stage,
udm.next_promo_stage_amt,
udm.next_promo_stage_point,
udm.point_tobe_granted,
udm.stage,
udm.stage_monthly,
udm.totalprc_this_year,
ub2.expired_point_this_month,
ub2.expired_point_next_month,
ub2.outcall_exc_flg,
ub2.outcall_hist_flg_3m,
ub2.subscription_flg,
ub2.ecpropensity,
ub2.ccpropensity,
ub2.acgelpropensity,
ub2.vc100propensity,
ub2.cluster5_cd,
ub2.cluster5_nm,
ub1.col146,
ub1.col147,
ub1.col148,
ub1.col149,
ub1.col150,
ub1.col151,
ub1.col152,
ub1.col153,
ub1.col154,
ub1.col155,
ub1.col156
FROM ua01_base_cim01kokya_v ckv
LEFT JOIN ua02_behavior01 ub1  
ON ckv.kokyano= ub1.kokyano
LEFT JOIN ua02_directmail udm
ON ckv.kokyano= udm.kokyano
LEFT JOIN ua02_behavior02 ub2
ON ckv.kokyano= ub2.kokyano
),
final as (
select
kokyano::varchar(60) as kokyano,
zipcode::varchar(60) as zipcode,
todofukenname::varchar(60) as todofukenname,
seibetukbn::varchar(60) as seibetukbn,
birthday::varchar(60) as birthday,
birthday_md::varchar(60) as birthday_md,
kokyakbn::varchar(60) as kokyakbn,
dmtenpoflg::varchar(60) as dmtenpoflg,
dmtsuhanflg::varchar(60) as dmtsuhanflg,
teltenpoflg::varchar(60) as teltenpoflg,
teltsuhanflg::varchar(60) as teltsuhanflg,
firstmediacode::varchar(60) as firstmediacode,
taikai_flg::varchar(60) as taikai_flg,
webno::varchar(60) as webno,
col1::number(18,0) as order_cnt_call_1m,
col2::float as order_amt_call_1m,
col3::number(18,0) as point_usage_cnt_call_1m,
col4::float as point_usage_amt_call_1m,
col5::number(18,0) as coupon_usage_cnt_call_1m,
col6::float as coupon_usage_amt_call_1m,
col7::number(18,0) as order_cnt_call_3m,
col8::float as order_amt_call_3m,
col9::number(18,0) as point_usage_cnt_call_3m,
col10::float as point_usage_amt_call_3m,
col11::number(18,0) as coupon_usage_cnt_call_3m,
col12::float as coupon_usage_amt_call_3m,
col13::number(18,0) as order_cnt_call_6m,
col14::float as order_amt_call_6m,
col15::number(18,0) as point_usage_cnt_call_6m,
col16::float as point_usage_amt_call_6m,
col17::number(18,0) as coupon_usage_cnt_call_6m,
col18::float as coupon_usage_amt_call_6m,
col19::number(18,0) as order_cnt_call_1y,
col20::float as order_amt_call_1y,
col21::number(18,0) as point_usage_cnt_call_1y,
col22::float as point_usage_amt_call_1y,
col23::number(18,0) as coupon_usage_cnt_call_1y,
col24::float as coupon_usage_amt_call_1y,
col25::number(18,0) as order_cnt_call_2y,
col26::float as order_amt_call_2y,
col27::number(18,0) as point_usage_cnt_call_2y,
col28::float as point_usage_amt_call_2y,
col29::number(18,0) as coupon_usage_cnt_call_2y,
col30::float as coupon_usage_amt_call_2y,
col31::number(18,0) as order_cnt_call,
col32::float as order_amt_call,
col33::number(18,0) as point_usage_cnt_call,
col34::float as point_usage_amt_call,
col35::number(18,0) as coupon_usage_cnt_call,
col36::float as coupon_usage_amt_call,
col37::number(18,0) as order_cnt_web_1m,
col38::float as order_amt_web_1m,
col39::number(18,0) as point_usage_cnt_web_1m,
col40::float as point_usage_amt_web_1m,
col41::number(18,0) as coupon_usage_cnt_web_1m,
col42::float as coupon_usage_amt_web_1m,
col43::number(18,0) as order_cnt_web_3m,
col44::float as order_amt_web_3m,
col45::number(18,0) as point_usage_cnt_web_3m,
col46::float as point_usage_amt_web_3m,
col47::number(18,0) as coupon_usage_cnt_web_3m,
col48::float as coupon_usage_amt_web_3m,
col49::number(18,0) as order_cnt_web_6m,
col50::float as order_amt_web_6m,
col51::number(18,0) as point_usage_cnt_web_6m,
col52::float as point_usage_amt_web_6m,
col53::number(18,0) as coupon_usage_cnt_web_6m,
col54::float as coupon_usage_amt_web_6m,
col55::number(18,0) as order_cnt_web_1y,
col56::float as order_amt_web_1y,
col57::number(18,0) as point_usage_cnt_web_1y,
col58::float as point_usage_amt_web_1y,
col59::number(18,0) as coupon_usage_cnt_web_1y,
col60::float as coupon_usage_amt_web_1y,
col61::number(18,0) as order_cnt_web_2y,
col62::float as order_amt_web_2y,
col63::number(18,0) as point_usage_cnt_web_2y,
col64::float as point_usage_amt_web_2y,
col65::number(18,0) as coupon_usage_cnt_web_2y,
col66::float as coupon_usage_amt_web_2y,
col67::number(18,0) as order_cnt_web,
col68::float as order_amt_web,
col69::number(18,0) as point_usage_cnt_web,
col70::float as point_usage_amt_web,
col71::number(18,0) as coupon_usage_cnt_web,
col72::float as coupon_usage_amt_web,
col73::number(18,0) as order_cnt_store_1m,
col74::float as order_amt_store_1m,
col75::number(18,0) as point_usage_cnt_store_1m,
col76::float as point_usage_amt_store_1m,
col77::number(18,0) as coupon_usage_cnt_store_1m,
col78::float as coupon_usage_amt_store_1m,
col79::number(18,0) as order_cnt_store_3m,
col80::float as order_amt_store_3m,
col81::number(18,0) as point_usage_cnt_store_3m,
col82::float as point_usage_amt_store_3m,
col83::number(18,0) as coupon_usage_cnt_store_3m,
col84::float as coupon_usage_amt_store_3m,
col85::number(18,0) as order_cnt_store_6m,
col86::float as order_amt_store_6m,
col87::number(18,0) as point_usage_cnt_store_6m,
col88::float as point_usage_amt_store_6m,
col89::number(18,0) as coupon_usage_cnt_store_6m,
col90::float as coupon_usage_amt_store_6m,
col91::number(18,0) as order_cnt_store_1y,
col92::float as order_amt_store_1y,
col93::number(18,0) as point_usage_cnt_store_1y,
col94::float as point_usage_amt_store_1y,
col95::number(18,0) as coupon_usage_cnt_store_1y,
col96::float as coupon_usage_amt_store_1y,
col97::number(18,0) as order_cnt_store_2y,
col98::float as order_amt_store_2y,
col99::number(18,0) as point_usage_cnt_store_2y,
col100::float as point_usage_amt_store_2y,
col101::number(18,0) as coupon_usage_cnt_store_2y,
col102::float as coupon_usage_amt_store_2y,
col103::number(18,0) as order_cnt_store,
col104::float as order_amt_store,
col105::number(18,0) as point_usage_cnt_store,
col106::float as point_usage_amt_store,
col107::number(18,0) as coupon_usage_cnt_store,
col108::float as coupon_usage_amt_store,
col109::number(18,0) as ltv_order_cnt_call,
col110::number(18,0) as ltv_order_cnt_web,
col111::number(18,0) as ltv_order_cnt_store,
col112::date as i_order_dt,
col113::date as l_order_dt,
col114::varchar(60) as i_channel,
col115::varchar(60) as l_channel,
col116::number(18,0) as lbag_reaction_cnt,
col117::date as i_lbag_order_dt,
col118::date as l_lbag_order_dt,
col119::number(18,0) as outlet_reaction_cnt,
col120::date as i_outlet_order_dt,
col121::date as l_outlet_order_dt,
col122::float as coupon_usage_ratio,
col123::float as coupon_usage_amt_average,
col124::date as i_coupon_usage_dt,
col125::date as l_coupon_usage_dt,
col126::float as point_usage_ratio,
col127::date as tradeup_date_vc100,
col128::date as tradedown_date_vc100,
col129::date as standard_new_date_vc100,
col130::date as premium_new_date_vc100,
col131::varchar(60) as trade_flag_vc100,
col132::date as tradeup_date_aid,
col133::date as tradedown_date_aid,
col134::date as standard_new_date_aid,
col135::date as premium_new_date_aid,
col136::varchar(60) as trade_flag_aid,
col137::date as tradeup_date_acgel,
col138::date as tradedown_date_acgel,
col139::date as standard_new_date_acgel,
col140::date as premium_new_date_acgel,
col141::varchar(60) as trade_flag_acgel,
col142::date as i_order_dt_inc_sample,
col143::date as l_order_dt_web,
col144::date as l_order_dt_call,
col145::date as l_order_dt_store,
register_dt::number(18,0) as register_dt,
biken_flg::number(18,0) as biken_flg,
register_card_flg::number(18,0) as register_card_flg,
other_adv_flg::number(18,0) as other_adv_flg,
main_store::varchar(60) as main_store,
familysale_class::number(18,0) as familysale_class,
main_channel_bddm::varchar(60) as main_channel_bddm,
promo_stage::varchar(18) as promo_stage,
next_promo_stage_amt::number(18,0) as next_promo_stage_amt,
next_promo_stage_point::number(18,0) as next_promo_stage_point,
point_tobe_granted::number(18,0) as point_tobe_granted,
stage::varchar(18) as stage,
stage_monthly::varchar(18) as stage_monthly,
totalprc_this_year::number(38,0) as totalprc_this_year,
expired_point_this_month::number(38,0) as expired_point_this_month,
expired_point_next_month::number(38,0) as expired_point_next_month,
outcall_exc_flg::varchar(1) as outcall_exc_flg,
outcall_hist_flg_3m::varchar(1) as outcall_hist_flg_3m,
subscription_flg::varchar(1) as subscription_flg,
ecpropensity::varchar(60) as ecpropensity,
ccpropensity::varchar(60) as ccpropensity,
acgelpropensity::varchar(60) as acgelpropensity,
vc100propensity::varchar(60) as vc100propensity,
cluster5_cd::number(18,0) as cluster5_cd,
cluster5_nm::varchar(300) as cluster5_nm,
col146::float as order_amt_call_1y_term_start,
col147::float as order_amt_web_1y_term_start,
col148::float as order_amt_store_1y_term_start,
col149::number(18,0) as order_cnt_call_this_year,
col150::number(18,0) as order_cnt_web_this_year,
col151::number(18,0) as order_cnt_store_this_year,
col152::number(18,0) as order_sku_cnt_1y,
col153::number(18,0) as order_sku_cnt_1y_term_start,
col154::number(18,0) as order_cnt_call_exc_sub_1y,
col155::number(18,0) as order_cnt_web_exc_sub_1y,
col156::number(18,0) as order_cnt_store_exc_sub_1y
from transformed
)
select * from final