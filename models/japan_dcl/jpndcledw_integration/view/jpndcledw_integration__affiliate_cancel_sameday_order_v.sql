with affiliate_cancel_sameday_order
as
(
    select * from {{ref('jpndcledw_integration__affiliate_cancel_sameday_order')}}
)
,

transformed
as
(
SELECT '0' AS sort_key
	,'顧客番号' AS diusrid
	,'WEBID' AS webid
	,'受領_ORDERID' AS rcv_orderid
	,'受領_ORDERDT' AS rcv_orderdt
	,'受領_PRICE' AS rcv_price
	,'DIORDERID' AS diorderid
	,'DSORDERDT' AS dsorderdt
	,'DITOTALPRC' AS ditotalprc
	,'DISEIKYUPRC' AS diseikyuprc
	,'DIORDERTAX' AS diordertax
	,'C_DIDISCOUNTPRC' AS c_didiscountprc
	,'C_DIDISCOUNTALL' AS c_didiscountall
	,'DIUSEPOINT' AS diusepoint
	,'DIHAISOPRC' AS dihaisoprc
	,'C_DICOLLECTPRC' AS c_dicollectprc
	,'C_DITOUJITSUHAISOPRC' AS c_ditoujitsuhaisoprc
	,'PRICE_ZEINUKI' AS price_zeinuki
	,'JUDGE_PRICE' AS judge_price
	,'PRICE_SABUN' AS price_sabun

UNION ALL

SELECT '1' AS sort_key
	,(trim((affiliate_cancel_sameday_order.diusrid)::TEXT))::CHARACTER VARYING AS diusrid
	,(trim((affiliate_cancel_sameday_order.webid)::TEXT))::CHARACTER VARYING AS webid
	,(trim((affiliate_cancel_sameday_order.rcv_orderid)::TEXT))::CHARACTER VARYING AS rcv_orderid
	,(trim((affiliate_cancel_sameday_order.rcv_orderdt)::TEXT))::CHARACTER VARYING AS rcv_orderdt
	,(trim(((affiliate_cancel_sameday_order.rcv_price)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS rcv_price
	,(trim(((affiliate_cancel_sameday_order.diorderid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS diorderid
	,(trim(((affiliate_cancel_sameday_order.dsorderdt)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS dsorderdt
	,(trim(((affiliate_cancel_sameday_order.ditotalprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS ditotalprc
	,(trim(((affiliate_cancel_sameday_order.diseikyuprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS diseikyuprc
	,(trim(((affiliate_cancel_sameday_order.diordertax)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS diordertax
	,(trim(((affiliate_cancel_sameday_order.c_didiscountprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS c_didiscountprc
	,(trim(((affiliate_cancel_sameday_order.c_didiscountall)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS c_didiscountall
	,(trim(((affiliate_cancel_sameday_order.diusepoint)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS diusepoint
	,(trim(((affiliate_cancel_sameday_order.dihaisoprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS dihaisoprc
	,(trim(((affiliate_cancel_sameday_order.c_dicollectprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS c_dicollectprc
	,(trim(((affiliate_cancel_sameday_order.c_ditoujitsuhaisoprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS c_ditoujitsuhaisoprc
	,(trim(((affiliate_cancel_sameday_order.price_zeinuki)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS price_zeinuki
	,(trim(((affiliate_cancel_sameday_order.judge_price)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS judge_price
	,(trim(((affiliate_cancel_sameday_order.price_sabun)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS price_sabun
FROM affiliate_cancel_sameday_order
ORDER BY 1)

select * from transformed