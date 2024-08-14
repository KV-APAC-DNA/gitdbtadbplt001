with affiliate_cancel_delivery
as (
	select *
	from {{ref('jpndcledw_integration__affiliate_cancel_delivery')}}
	) ,

	transformed AS (
		SELECT '0' AS sort_key
			,'ACHIEVEMENT' AS achievement
			,'CLICK_DT' AS click_dt
			,'ACCRUAL_DT' AS accrual_dt
			,'ASP' AS asp
			,'UNIQUE_ID' AS unique_id
			,'MEDIA_NAME' AS media_name
			,'ASP_CONTROL_NO' AS asp_control_no
			,'SALE_NUM' AS sale_num
			,'AMOUNT_INCLUDING_TAX' AS amount_including_tax
			,'AMOUNT_EXCLUDED_TAX' AS amount_excluded_tax
			,'ORDERDATE' AS orderdate
			,'WEBID' AS webid
			,'受領_ORDERID' AS rcv_orderid
			,'受領_ORDERDT' AS rcv_orderdt
			,'受領_PRICE' AS rcv_price
			,'CI_NEXT_受注金額（税抜き）' AS cnxt_price_tax_excl
			,'CI_NEXT_受注金額（税込）' AS cnxt_ditotalprc
			,'CI_NEXT_消費税額' AS cnxt_diordertax
			,'CI_NEXT_割引金額' AS cnxt_c_didiscountprc
			,'CI_NEXT_値引金額' AS cnxt_c_didiscountall
			,'CI_NEXT_ポイント使用額' AS cnxt_diusepoint
			,'CI_NEXT_配送料金' AS cnxt_dihaisoprc
			,'CI_NEXT_代引き手数料' AS cnxt_c_dicollectprc
			,'CI_NEXT_当日配送手数料' AS cnxt_c_ditoujitsuhaisoprc
			,'CI_NEXT_請求金額（税込み）' AS cnxt_diseikyuprc
			,'CI_NEXT_オーダーID' AS cnxt_diorderid
			,'顧客番号' AS diusrid
			,'WEBID_1' AS webid_1
			,'比較用金額' AS judge_price
			,'受領_PRICEと比較用金額の差分' AS price_sabun
			,'比較ステータス' AS cmpr_sts
			,'同日受注存在' AS samday_odr_flg
		
		UNION ALL
		
		SELECT '1' AS sort_key
			,(trim((affiliate_cancel_delivery.achievement)::TEXT))::CHARACTER VARYING AS achievement
			,(trim((affiliate_cancel_delivery.click_dt)::TEXT))::CHARACTER VARYING AS click_dt
			,(trim((affiliate_cancel_delivery.accrual_dt)::TEXT))::CHARACTER VARYING AS accrual_dt
			,(trim((affiliate_cancel_delivery.asp)::TEXT))::CHARACTER VARYING AS asp
			,(trim((affiliate_cancel_delivery.unique_id)::TEXT))::CHARACTER VARYING AS unique_id
			,(trim((affiliate_cancel_delivery.media_name)::TEXT))::CHARACTER VARYING AS media_name
			,(trim((affiliate_cancel_delivery.asp_control_no)::TEXT))::CHARACTER VARYING AS asp_control_no
			,(trim(((affiliate_cancel_delivery.sale_num)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS sale_num
			,(trim(((affiliate_cancel_delivery.amount_including_tax)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS amount_including_tax
			,(trim(((affiliate_cancel_delivery.amount_excluded_tax)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS amount_excluded_tax
			,(trim((affiliate_cancel_delivery.orderdate)::TEXT))::CHARACTER VARYING AS orderdate
			,(trim((affiliate_cancel_delivery.webid)::TEXT))::CHARACTER VARYING AS webid
			,(trim((affiliate_cancel_delivery.rcv_orderid)::TEXT))::CHARACTER VARYING AS rcv_orderid
			,(trim((affiliate_cancel_delivery.rcv_orderdt)::TEXT))::CHARACTER VARYING AS rcv_orderdt
			,(trim(((affiliate_cancel_delivery.rcv_price)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS rcv_price
			,(trim(((affiliate_cancel_delivery.cnxt_price_tax_excl)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_price_tax_excl
			,(trim(((affiliate_cancel_delivery.cnxt_ditotalprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_ditotalprc
			,(trim(((affiliate_cancel_delivery.cnxt_diordertax)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_diordertax
			,(trim(((affiliate_cancel_delivery.cnxt_c_didiscountprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_c_didiscountprc
			,(trim(((affiliate_cancel_delivery.cnxt_c_didiscountall)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_c_didiscountall
			,(trim(((affiliate_cancel_delivery.cnxt_diusepoint)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_diusepoint
			,(trim(((affiliate_cancel_delivery.cnxt_dihaisoprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_dihaisoprc
			,(trim(((affiliate_cancel_delivery.cnxt_c_dicollectprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_c_dicollectprc
			,(trim(((affiliate_cancel_delivery.cnxt_c_ditoujitsuhaisoprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_c_ditoujitsuhaisoprc
			,(trim(((affiliate_cancel_delivery.cnxt_diseikyuprc)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_diseikyuprc
			,(trim(((affiliate_cancel_delivery.cnxt_diorderid)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS cnxt_diorderid
			,(trim((affiliate_cancel_delivery.diusrid)::TEXT))::CHARACTER VARYING AS diusrid
			,(trim((affiliate_cancel_delivery.webid_1)::TEXT))::CHARACTER VARYING AS webid_1
			,(trim(((affiliate_cancel_delivery.judge_price)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS judge_price
			,(trim(((affiliate_cancel_delivery.price_sabun)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS price_sabun
			,(trim((affiliate_cancel_delivery.cmpr_sts)::TEXT))::CHARACTER VARYING AS cmpr_sts
			,(trim((affiliate_cancel_delivery.samday_odr_flg)::TEXT))::CHARACTER VARYING AS samday_odr_flg
		FROM affiliate_cancel_delivery
		ORDER BY 1
			,6
			,4
		)

SELECT *
FROM transformed
