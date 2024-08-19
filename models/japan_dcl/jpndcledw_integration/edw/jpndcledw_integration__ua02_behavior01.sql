with 
wk02_ua02_behavior01_channel_web as (
select * from {{ ref('jpndcledw_integration__wk02_ua02_behavior01_channel_web') }}
),
ua01_base_cim01kokya_v as (
select * from {{ ref('jpndcledw_integration__ua01_base_cim01kokya_v') }}
),
wk09_ua02_behavior01_trade as (
select * from {{ ref('jpndcledw_integration__wk09_ua02_behavior01_trade') }}
),
wk03_ua02_behavior01_channel_store as (
select * from {{ ref('jpndcledw_integration__wk03_ua02_behavior01_channel_store') }}
),
wk06_ua02_behavior01_order_reaction as (
select * from {{ ref('jpndcledw_integration__wk06_ua02_behavior01_order_reaction') }}
),
wk25_ua02_behavior01_order_sku_cnt as (
select * from {{ ref('jpndcledw_integration__wk25_ua02_behavior01_order_sku_cnt') }}
),
wk17_ua02_behavior01_i_order_dt_inc_sample as (
select * from  {{ ref('jpndcledw_integration__wk17_ua02_behavior01_i_order_dt_inc_sample') }}
),
wk07_ua02_behavior01_coupon_usage as (
select * from {{ ref('jpndcledw_integration__wk07_ua02_behavior01_coupon_usage') }}
),
wk18_ua02_behavior01_l_order_dt_channel as (
select * from {{ ref('jpndcledw_integration__wk18_ua02_behavior01_l_order_dt_channel') }}
),
WK26_UA02_BEHAVIOR01_ORDER_CNT_CHANNEL_EXC_SUB as (
select * from {{ ref('jpndcledw_integration__wk26_ua02_behavior01_order_cnt_channel_exc_sub') }}
),
wk04_ua02_behavior01_order_attr as (
select * from {{ ref('jpndcledw_integration__wk04_ua02_behavior01_order_attr') }}
),
wk24_ua02_behavior01_order_cnt_this_year as (
select * from {{ ref('jpndcledw_integration__wk24_ua02_behavior01_order_cnt_this_year') }}
),
wk23_ua02_behavior01_order_amt_term_start as (
select * from {{ ref('jpndcledw_integration__wk23_ua02_behavior01_order_amt_term_start') }}
),
wk05_ua02_behavior01_order_channel as (
select * from  {{ ref('jpndcledw_integration__wk05_ua02_behavior01_order_channel') }}
),
wk08_ua02_behavior01_point_usage as (
select * from  {{ ref('jpndcledw_integration__wk08_ua02_behavior01_point_usage') }}
),
wk01_ua02_behavior01_channel_call as (
select * from  {{ ref('jpndcledw_integration__wk01_ua02_behavior01_channel_call') }}
),
transformed as (
SELECT 
DISTINCT 
ckv.kokyano 	 
,NVL(a.Order_Cnt_call_1m,0) AS Order_Cnt_call_1m 
,NVL(a.Order_Amt_call_1m,0) AS Order_Amt_call_1m
,NVL(a.Point_Usage_Cnt_call_1m,0) AS Point_Usage_Cnt_call_1m
,NVL(a.Point_Usage_Amt_call_1m,0) AS  Point_Usage_Amt_call_1m
,NVL(a.Coupon_Usage_Cnt_call_1m,0) AS Coupon_Usage_Cnt_call_1m
,NVL(a.Coupon_Usage_Amt_call_1m,0) AS Coupon_Usage_Amt_call_1m
,NVL(a.Order_Cnt_call_3m,0) AS Order_Cnt_call_3m
,NVL(a.Order_Amt_call_3m,0) AS Order_Amt_call_3m
,NVL(a.Point_Usage_Cnt_call_3m,0) AS Point_Usage_Cnt_call_3m
,NVL(a.Point_Usage_Amt_call_3m,0) AS Point_Usage_Amt_call_3m
,NVL(a.Coupon_Usage_Cnt_call_3m,0) AS Coupon_Usage_Cnt_call_3m
,NVL(a.Coupon_Usage_Amt_call_3m,0) AS Coupon_Usage_Amt_call_3m
,NVL(a.Order_Cnt_call_6m,0) AS Order_Cnt_call_6m
,NVL(a.Order_Amt_call_6m,0) AS Order_Amt_call_6m
,NVL(a.Point_Usage_Cnt_call_6m,0) AS Point_Usage_Cnt_call_6m
,NVL(a.Point_Usage_Amt_call_6m,0) AS Point_Usage_Amt_call_6m
,NVL(a.Coupon_Usage_Cnt_call_6m,0) AS Coupon_Usage_Cnt_call_6m
,NVL(a.Coupon_Usage_Amt_call_6m,0) AS Coupon_Usage_Amt_call_6m
,NVL(a.Order_Cnt_call_1y,0) AS Order_Cnt_call_1y
,NVL(a.Order_Amt_call_1y,0) AS Order_Amt_call_1y
,NVL(a.Point_Usage_Cnt_call_1y,0) AS Point_Usage_Cnt_call_1y
,NVL(a.Point_Usage_Amt_call_1y,0) AS Point_Usage_Amt_call_1y
,NVL(a.Coupon_Usage_Cnt_call_1y,0) AS Coupon_Usage_Cnt_call_1y
,NVL(a.Coupon_Usage_Amt_call_1y,0) AS Coupon_Usage_Amt_call_1y
,NVL(a.Order_Cnt_call_2y,0) AS Order_Cnt_call_2y
,NVL(a.Order_Amt_call_2y,0) AS Order_Amt_call_2y
,NVL(a.Point_Usage_Cnt_call_2y,0) AS Point_Usage_Cnt_call_2y
,NVL(a.Point_Usage_Amt_call_2y,0) AS Point_Usage_Amt_call_2y
,NVL(a.Coupon_Usage_Cnt_call_2y,0) AS Coupon_Usage_Cnt_call_2y
,NVL(a.Coupon_Usage_Amt_call_2y,0) AS Coupon_Usage_Amt_call_2y
,NVL(a.Order_Cnt_call,0) AS Order_Cnt_call
,NVL(a.Order_Amt_call,0) AS Order_Amt_call
,NVL(a.Point_Usage_Cnt_call,0) AS Point_Usage_Cnt_call
,NVL(a.Point_Usage_Amt_call,0) AS Point_Usage_Amt_call
,NVL(a.Coupon_Usage_Cnt_call,0) AS Coupon_Usage_Cnt_call
,NVL(a.Coupon_Usage_Amt_call,0) AS   Coupon_Usage_Amt_call
,NVL(b.Order_Cnt_web_1m,0) AS Order_Cnt_web_1m
,NVL(b.Order_Amt_web_1m,0) AS Order_Amt_web_1m
,NVL(b.Point_Usage_Cnt_web_1m,0) AS  Point_Usage_Cnt_web_1m
,NVL(b.Point_Usage_Amt_web_1m,0) AS Point_Usage_Amt_web_1m
,NVL(b.Coupon_Usage_Cnt_web_1m,0) AS Coupon_Usage_Cnt_web_1m
,NVL(b.Coupon_Usage_Amt_web_1m,0) AS Coupon_Usage_Amt_web_1m
,NVL(b.Order_Cnt_web_3m,0) AS Order_Cnt_web_3m
,NVL(b.Order_Amt_web_3m,0) AS Order_Amt_web_3m
,NVL(b.Point_Usage_Cnt_web_3m,0) AS Point_Usage_Cnt_web_3m
,NVL(b.Point_Usage_Amt_web_3m,0) AS Point_Usage_Amt_web_3m
,NVL(b.Coupon_Usage_Cnt_web_3m,0) AS Coupon_Usage_Cnt_web_3m
,NVL(b.Coupon_Usage_Amt_web_3m,0) AS Coupon_Usage_Amt_web_3m
,NVL(b.Order_Cnt_web_6m,0) AS Order_Cnt_web_6m
,NVL(b.Order_Amt_web_6m,0) AS Order_Amt_web_6m
,NVL(b.Point_Usage_Cnt_web_6m,0) AS Point_Usage_Cnt_web_6m
,NVL(b.Point_Usage_Amt_web_6m,0) AS Point_Usage_Amt_web_6m
,NVL(b.Coupon_Usage_Cnt_web_6m,0) AS Coupon_Usage_Cnt_web_6m
,NVL(b.Coupon_Usage_Amt_web_6m,0) AS Coupon_Usage_Amt_web_6m
,NVL(b.Order_Cnt_web_1y,0) AS Order_Cnt_web_1y
,NVL(b.Order_Amt_web_1y,0) AS Order_Amt_web_1y
,NVL(b.Point_Usage_Cnt_web_1y,0) AS Point_Usage_Cnt_web_1y
,NVL(b.Point_Usage_Amt_web_1y,0) AS Point_Usage_Amt_web_1y
,NVL(b.Coupon_Usage_Cnt_web_1y,0) AS Coupon_Usage_Cnt_web_1y
,NVL(b.Coupon_Usage_Amt_web_1y,0) AS Coupon_Usage_Amt_web_1y
,NVL(b.Order_Cnt_web_2y,0) AS Order_Cnt_web_2y
,NVL(b.Order_Amt_web_2y,0) AS Order_Amt_web_2y
,NVL(b.Point_Usage_Cnt_web_2y,0) AS Point_Usage_Cnt_web_2y
,NVL(b.Point_Usage_Amt_web_2y,0) AS Point_Usage_Amt_web_2y
,NVL(b.Coupon_Usage_Cnt_web_2y,0) AS Coupon_Usage_Cnt_web_2y
,NVL(b.Coupon_Usage_Amt_web_2y,0) AS Coupon_Usage_Amt_web_2y
,NVL(b.Order_Cnt_web,0) AS Order_Cnt_web
,NVL(b.Order_Amt_web,0) AS Order_Amt_web
,NVL(b.Point_Usage_Cnt_web,0) AS Point_Usage_Cnt_web
,NVL(b.Point_Usage_Amt_web,0) AS Point_Usage_Amt_web
,NVL(b.Coupon_Usage_Cnt_web,0) AS Coupon_Usage_Cnt_web
,NVL(b.Coupon_Usage_Amt_web,0) AS Coupon_Usage_Amt_web
,NVL(c.Order_Cnt_store_1m,0) AS Order_Cnt_store_1m
,NVL(c.Order_Amt_store_1m,0) AS Order_Amt_store_1m
,NVL(c.Point_Usage_Cnt_store_1m,0) AS Point_Usage_Cnt_store_1m
,NVL(c.Point_Usage_Amt_store_1m,0) AS Point_Usage_Amt_store_1m
,NVL(c.Coupon_Usage_Cnt_store_1m,0) AS Coupon_Usage_Cnt_store_1m
,NVL(c.Coupon_Usage_Amt_store_1m,0) AS Coupon_Usage_Amt_store_1m
,NVL(c.Order_Cnt_store_3m,0) AS Order_Cnt_store_3m
,NVL(c.Order_Amt_store_3m,0) AS Order_Amt_store_3m
,NVL(c.Point_Usage_Cnt_store_3m,0) AS Point_Usage_Cnt_store_3m
,NVL(c.Point_Usage_Amt_store_3m,0) AS Point_Usage_Amt_store_3m
,NVL(c.Coupon_Usage_Cnt_store_3m,0) AS Coupon_Usage_Cnt_store_3m
,NVL(c.Coupon_Usage_Amt_store_3m,0) AS Coupon_Usage_Amt_store_3m
,NVL(c.Order_Cnt_store_6m,0) AS Order_Cnt_store_6m
,NVL(c.Order_Amt_store_6m,0) AS Order_Amt_store_6m
,NVL(c.Point_Usage_Cnt_store_6m,0) AS Point_Usage_Cnt_store_6m
,NVL(c.Point_Usage_Amt_store_6m,0) AS Point_Usage_Amt_store_6m
,NVL(c.Coupon_Usage_Cnt_store_6m,0) AS Coupon_Usage_Cnt_store_6m
,NVL(c.Coupon_Usage_Amt_store_6m,0) AS Coupon_Usage_Amt_store_6m
,NVL(c.Order_Cnt_store_1y,0) AS Order_Cnt_store_1y
,NVL(c.Order_Amt_store_1y,0) AS Order_Amt_store_1y
,NVL(c.Point_Usage_Cnt_store_1y,0) AS Point_Usage_Cnt_store_1y
,NVL(c.Point_Usage_Amt_store_1y,0) AS Point_Usage_Amt_store_1y
,NVL(c.Coupon_Usage_Cnt_store_1y,0) AS Coupon_Usage_Cnt_store_1y
,NVL(c.Coupon_Usage_Amt_store_1y,0) AS Coupon_Usage_Amt_store_1y
,NVL(c.Order_Cnt_store_2y,0) AS Order_Cnt_store_2y
,NVL(c.Order_Amt_store_2y,0) AS Order_Amt_store_2y
,NVL(c.Point_Usage_Cnt_store_2y,0) AS Point_Usage_Cnt_store_2y
,NVL(c.Point_Usage_Amt_store_2y,0) AS Point_Usage_Amt_store_2y
,NVL(c.Coupon_Usage_Cnt_store_2y,0) AS Coupon_Usage_Cnt_store_2y
,NVL(c.Coupon_Usage_Amt_store_2y,0) AS Coupon_Usage_Amt_store_2y
,NVL(c.Order_Cnt_store,0) AS Order_Cnt_store
,NVL(c.Order_Amt_store,0) AS Order_Amt_store
,NVL(c.Point_Usage_Cnt_store,0) AS Point_Usage_Cnt_store
,NVL(c.Point_Usage_Amt_store,0) AS Point_Usage_Amt_store
,NVL(c.Coupon_Usage_Cnt_store,0) AS Coupon_Usage_Cnt_store
,NVL(c.Coupon_Usage_Amt_store,0) AS Coupon_Usage_Amt_store
,NVL(	d.LTV_Order_Cnt_Call,0) AS LTV_Order_Cnt_Call
,NVL(	d.LTV_Order_Cnt_Web,0) AS LTV_Order_Cnt_Web
,NVL(	d.LTV_Order_Cnt_Store,0) AS LTV_Order_Cnt_Store
,	d.I_Order_dt -- if date, null is ok
,	d.L_Order_dt 
,	e.I_Channel -- if cahnnel, null is ok
,	e.L_Channel 
,NVL(	f.Lbag_Reaction_Cnt,0) AS Lbag_Reaction_Cnt
,	f.I_Lbag_Order_dt 
,	f.L_Lbag_Order_dt 
,NVL(	f.Outlet_Reaction_Cnt,0) AS Outlet_Reaction_Cnt
,	f.I_Outlet_Order_dt
,	f.L_Outlet_Order_dt
,NVL(	g.Coupon_Usage_Ratio,0) AS Coupon_Usage_Ratio
,NVL(	g.Coupon_Usage_Amt_Average,0) AS Coupon_Usage_Amt_Average
,	g.I_Coupon_Usage_dt 
,	g.L_Coupon_Usage_dt 
,NVL(	h.Point_Usage_Ratio,0) AS Point_Usage_Ratio
,	i.Tradeup_Date_VC100 -- if date, null is ok
,	i.Tradedown_Date_VC100
,	i.Standard_New_Date_VC100
,	i.Premium_New_Date_VC100
,	i.Trade_flag_VC100 -- if date, null is ok
,	i.Tradeup_Date_AID
,	i.Tradedown_Date_AID
,	i.Standard_New_Date_AID
,	i.Premium_New_Date_AID
,	i.Trade_flag_AID
,	i.Tradeup_Date_ACGEL
,	i.Tradedown_Date_ACGEL
,	i.Standard_New_Date_ACGEL
,	i.Premium_New_Date_ACGEL
,	i.Trade_flag_ACGEL 
,	j.I_Order_dt_inc_sample
,	k.L_order_dt_web
,	k.L_order_dt_call
,	k.L_order_dt_store
,NVL(l.order_amt_call_1y_term_start,0) AS order_amt_call_1y_term_start
,NVL(l.order_amt_web_1y_term_start,0) AS order_amt_web_1y_term_start
,NVL(l.order_amt_store_1y_term_start,0) AS order_amt_store_1y_term_start
,NVL(m.Order_Cnt_call_this_year,0) AS Order_Cnt_call_this_year
,NVL(m.Order_Cnt_web_this_year,0) AS Order_Cnt_web_this_year
,NVL(m.Order_Cnt_store_this_year,0) AS Order_Cnt_store_this_year
,NVL(n.order_sku_cnt_1y,0) AS order_sku_cnt_1y
,NVL(n.order_sku_cnt_1y_term_start,0) AS order_sku_cnt_1y_term_start
,NVL(o.order_cnt_call_exc_sub_1y,0) AS order_cnt_call_exc_sub_1y
,NVL(o.order_cnt_web_exc_sub_1y,0) AS order_cnt_web_exc_sub_1y
,NVL(o.order_cnt_store_exc_sub_1y,0) AS order_cnt_store_exc_sub_1y 
FROM  ua01_bASe_cim01kokya_v ckv 
LEFT JOIN WK01_UA02_behavior01_channel_call a
ON ckv.kokyano=a.customer_no
LEFT JOIN WK02_UA02_behavior01_channel_web b
ON ckv.kokyano=b.customer_no
LEFT JOIN WK03_UA02_behavior01_channel_store c
ON ckv.kokyano=c.customer_no
LEFT JOIN WK04_UA02_behavior01_Order_Attr d
ON ckv.kokyano=d.customer_no
LEFT JOIN WK05_UA02_behavior01_Order_Channel e
ON ckv.kokyano=e.customer_no
LEFT JOIN wk06_ua02_behavior01_order_reaction f
ON ckv.kokyano=f.customer_no
LEFT JOIN wk07_ua02_behavior01_coupon_usage g
ON ckv.kokyano=g.customer_no
LEFT JOIN wk08_ua02_behavior01_point_usage h
ON ckv.kokyano=h.customer_no
LEFT JOIN wk09_ua02_behavior01_trade i
ON ckv.kokyano=i.customer_no
LEFT JOIN wk17_ua02_behavior01_I_Order_dt_inc_sample j
ON ckv.kokyano=j.customer_no
LEFT JOIN wk18_ua02_behavior01_L_order_dt_channel k
ON ckv.kokyano=k.customer_no
LEFT JOIN wk23_ua02_behavior01_order_amt_term_start l
ON ckv.kokyano=l.customer_no
LEFT JOIN wk24_ua02_behavior01_order_cnt_this_year m
ON ckv.kokyano=m.customer_no
LEFT JOIN wk25_ua02_behavior01_order_sku_cnt n
ON ckv.kokyano=n.customer_no
LEFT JOIN wk26_ua02_behavior01_order_cnt_channel_exc_sub o
ON ckv.kokyano=o.customer_no
),
final as (
select
kokyano::varchar(60) as kokyano,
Order_Cnt_call_1m ::number(18,0) as col1,
Order_Amt_call_1m::float as col2,
Point_Usage_Cnt_call_1m::number(18,0) as col3,
Point_Usage_Amt_call_1m::float as col4,
Coupon_Usage_Cnt_call_1m::number(18,0) as col5,
Coupon_Usage_Amt_call_1m::float as col6,
Order_Cnt_call_3m::number(18,0) as col7,
Order_Amt_call_3m::float as col8,
Point_Usage_Cnt_call_3m::number(18,0) as col9,
Point_Usage_Amt_call_3m::float as col10,
Coupon_Usage_Cnt_call_3m::number(18,0) as col11,
Coupon_Usage_Amt_call_3m::float as col12,
Order_Cnt_call_6m::number(18,0) as col13,
Order_Amt_call_6m::float as col14,
Point_Usage_Cnt_call_6m::number(18,0) as col15,
Point_Usage_Amt_call_6m::float as col16,
Coupon_Usage_Cnt_call_6m::number(18,0) as col17,
Coupon_Usage_Amt_call_6m::float as col18,
Order_Cnt_call_1y::number(18,0) as col19,
Order_Amt_call_1y::float as col20,
Point_Usage_Cnt_call_1y::number(18,0) as col21,
Point_Usage_Amt_call_1y::float as col22,
Coupon_Usage_Cnt_call_1y::number(18,0) as col23,
Coupon_Usage_Amt_call_1y::float as col24,
Order_Cnt_call_2y::number(18,0) as col25,
Order_Amt_call_2y::float as col26,
Point_Usage_Cnt_call_2y::number(18,0) as col27,
Point_Usage_Amt_call_2y::float as col28,
Coupon_Usage_Cnt_call_2y::number(18,0) as col29,
Coupon_Usage_Amt_call_2y::float as col30,
Order_Cnt_call::number(18,0) as col31,
Order_Amt_call::float as col32,
Point_Usage_Cnt_call::number(18,0) as col33,
Point_Usage_Amt_call::float as col34,
Coupon_Usage_Cnt_call::number(18,0) as col35,
Coupon_Usage_Amt_call::float as col36,
Order_Cnt_web_1m::number(18,0) as col37,
Order_Amt_web_1m::float as col38,
Point_Usage_Cnt_web_1m::number(18,0) as col39,
Point_Usage_Amt_web_1m::float as col40,
Coupon_Usage_Cnt_web_1m::number(18,0) as col41,
Coupon_Usage_Amt_web_1m::float as col42,
Order_Cnt_web_3m::number(18,0) as col43,
Order_Amt_web_3m::float as col44,
Point_Usage_Cnt_web_3m::number(18,0) as col45,
Point_Usage_Amt_web_3m::float as col46,
Coupon_Usage_Cnt_web_3m::number(18,0) as col47,
Coupon_Usage_Amt_web_3m::float as col48,
Order_Cnt_web_6m::number(18,0) as col49,
Order_Amt_web_6m::float as col50,
Point_Usage_Cnt_web_6m::number(18,0) as col51,
Point_Usage_Amt_web_6m::float as col52,
Coupon_Usage_Cnt_web_6m::number(18,0) as col53,
Coupon_Usage_Amt_web_6m::float as col54,
Order_Cnt_web_1y::number(18,0) as col55,
Order_Amt_web_1y::float as col56,
Point_Usage_Cnt_web_1y::number(18,0) as col57,
Point_Usage_Amt_web_1y::float as col58,
Coupon_Usage_Cnt_web_1y::number(18,0) as col59,
Coupon_Usage_Amt_web_1y::float as col60,
Order_Cnt_web_2y::number(18,0) as col61,
Order_Amt_web_2y::float as col62,
Point_Usage_Cnt_web_2y::number(18,0) as col63,
Point_Usage_Amt_web_2y::float as col64,
Coupon_Usage_Cnt_web_2y::number(18,0) as col65,
Coupon_Usage_Amt_web_2y::float as col66,
Order_Cnt_web::number(18,0) as col67,
Order_Amt_web::float as col68,
Point_Usage_Cnt_web::number(18,0) as col69,
Point_Usage_Amt_web::float as col70,
Coupon_Usage_Cnt_web::number(18,0) as col71,
Coupon_Usage_Amt_web::float as col72,
Order_Cnt_store_1m::number(18,0) as col73,
Order_Amt_store_1m::float as col74,
Point_Usage_Cnt_store_1m::number(18,0) as col75,
Point_Usage_Amt_store_1m::float as col76,
Coupon_Usage_Cnt_store_1m::number(18,0) as col77,
Coupon_Usage_Amt_store_1m::float as col78,
Order_Cnt_store_3m::number(18,0) as col79,
Order_Amt_store_3m::float as col80,
Point_Usage_Cnt_store_3m::number(18,0) as col81,
Point_Usage_Amt_store_3m::float as col82,
Coupon_Usage_Cnt_store_3m::number(18,0) as col83,
Coupon_Usage_Amt_store_3m::float as col84,
Order_Cnt_store_6m::number(18,0) as col85,
Order_Amt_store_6m::float as col86,
Point_Usage_Cnt_store_6m::number(18,0) as col87,
Point_Usage_Amt_store_6m::float as col88,
Coupon_Usage_Cnt_store_6m::number(18,0) as col89,
Coupon_Usage_Amt_store_6m::float as col90,
Order_Cnt_store_1y::number(18,0) as col91,
Order_Amt_store_1y::float as col92,
Point_Usage_Cnt_store_1y::number(18,0) as col93,
Point_Usage_Amt_store_1y::float as col94,
Coupon_Usage_Cnt_store_1y::number(18,0) as col95,
Coupon_Usage_Amt_store_1y::float as col96,
Order_Cnt_store_2y::number(18,0) as col97,
Order_Amt_store_2y::float as col98,
Point_Usage_Cnt_store_2y::number(18,0) as col99,
Point_Usage_Amt_store_2y::float as col100,
Coupon_Usage_Cnt_store_2y::number(18,0) as col101,
Coupon_Usage_Amt_store_2y::float as col102,
Order_Cnt_store::number(18,0) as col103,
Order_Amt_store::float as col104,
Point_Usage_Cnt_store::number(18,0) as col105,
Point_Usage_Amt_store::float as col106,
Coupon_Usage_Cnt_store::number(18,0) as col107,
Coupon_Usage_Amt_store::float as col108,
LTV_Order_Cnt_Call::number(18,0) as col109,
LTV_Order_Cnt_Web::number(18,0) as col110,
LTV_Order_Cnt_Store::number(18,0) as col111,
I_Order_dt::date as col112,
L_Order_dt::date as col113,
I_Channel::varchar(60) as col114,
L_Channel ::varchar(60) as col115,
Lbag_Reaction_Cnt::number(18,0) as col116,
I_Lbag_Order_dt::date as col117,
L_Lbag_Order_dt::date as col118,
Outlet_Reaction_Cnt::number(18,0) as col119,
I_Outlet_Order_dt::date as col120,
L_Outlet_Order_dt::date as col121,
Coupon_Usage_Ratio::float as col122,
Coupon_Usage_Amt_Average::float as col123,
I_Coupon_Usage_dt::date as col124,
L_Coupon_Usage_dt::date as col125,
Point_Usage_Ratio::float as col126,
Tradeup_Date_VC100::date as col127,
Tradedown_Date_VC100::date as col128,
Standard_New_Date_VC100::date as col129,
Premium_New_Date_VC100::date as col130,
Trade_flag_VC100 ::varchar(60) as col131,
Tradeup_Date_AID::date as col132,
Tradedown_Date_AID::date as col133,
Standard_New_Date_AID::date as col134,
Premium_New_Date_AID::date as col135,
Trade_flag_AID::varchar(60) as col136,
Tradeup_Date_ACGEL::date as col137,
Tradedown_Date_ACGEL::date as col138,
Standard_New_Date_ACGEL::date as col139,
Premium_New_Date_ACGEL::date as col140,
Trade_flag_ACGEL::varchar(60) as col141,
I_Order_dt_inc_sample::date as col142,
L_order_dt_web::date as col143,
L_order_dt_call::date as col144,
L_order_dt_store::date as col145,
order_amt_call_1y_term_start::float as col146,
order_amt_web_1y_term_start::float as col147,
order_amt_store_1y_term_start::float as col148,
Order_Cnt_call_this_year::number(18,0) as col149,
Order_Cnt_web_this_year::number(18,0) as col150,
Order_Cnt_store_this_year::number(18,0) as col151,
order_sku_cnt_1y::number(18,0) as col152,
order_sku_cnt_1y_term_start::number(18,0) as col153,
order_cnt_call_exc_sub_1y::number(18,0) as col154,
order_cnt_web_exc_sub_1y::number(18,0) as col155,
order_cnt_store_exc_sub_1y::number(18,0) as col156
from transformed
)
select * from final