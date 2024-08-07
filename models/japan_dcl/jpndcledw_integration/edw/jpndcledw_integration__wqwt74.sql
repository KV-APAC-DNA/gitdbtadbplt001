with wt74itemstrategy_sale
as
(
    select * from dev_dna_core.snapjpdcledw_integration.wt74itemstrategy_sale 
)
,
transformed
as
(
    SELECT trim(wt74itemstrategy_sale.shukanengetubi) AS "出荷年月日"
	,wt74itemstrategy_sale.konyuchubuncode AS "通信経路大区分"
	,wt74itemstrategy_sale.channel AS "チャネル"
	,wt74itemstrategy_sale.torikeikbn AS "取引形態区分"
	,wt74itemstrategy_sale.tokuicode AS "得意先"
	,wt74itemstrategy_sale.hanbaiitem AS "販売商品"
	,wt74itemstrategy_sale.kouseiitem AS "商品構成"
	,wt74itemstrategy_sale.kosecode AS "在庫商品コード"
	,wt74itemstrategy_sale.juchkbn AS "受注区分コード"
	,wt74itemstrategy_sale.juchkbncname AS "c受注区分名"
	,wt74itemstrategy_sale.suryo AS "数量"
	,wt74itemstrategy_sale.hensu AS "返品数量"
	,wt74itemstrategy_sale.urisuryo AS "純売上数量"
	,wt74itemstrategy_sale.anbunmaegaku_tyouseimae AS "按分前金額_調整前"
	,wt74itemstrategy_sale.meisainukikingaku_tyouseimae AS "明細金額_調整前"
	,wt74itemstrategy_sale.anbunmaegaku_tyouseigo AS "按分前金額_調整後"
	,wt74itemstrategy_sale.meisainukikingaku_tyouseigo AS "明細金額_調整後"
	,wt74itemstrategy_sale.anbunmeisainukikingaku AS "伝票値引按分後売上金額"
	,wt74itemstrategy_sale.channel1 AS "チャネル大区分"
	,wt74itemstrategy_sale.channel2 AS "チャネル中区分"
	,wt74itemstrategy_sale.channel3 AS "チャネル小区分"
	,wt74itemstrategy_sale.bmn_hyouji_cd AS "売上計上部門コード"
	,wt74itemstrategy_sale.bmn_nms AS "売上計上部門名"
	,wt74itemstrategy_sale.henreasoncode AS "返品理由コード"
	,wt74itemstrategy_sale.henreasonname AS "返品理由"
	,wt74itemstrategy_sale.sokoname AS "店舗名"
	,wt74itemstrategy_sale.kubun2 AS "区分2"
	,wt74itemstrategy_sale.syohingun AS "商品群"
	,wt74itemstrategy_sale.jyuutenitem AS "重点商品"
	,wt74itemstrategy_sale.item_bunr_val1 AS "品目分類値コード1"
	,wt74itemstrategy_sale.item_bunr_val2 AS "品目分類値コード2"
	,wt74itemstrategy_sale.item_bunr_val3 AS "品目分類値コード3"
	,wt74itemstrategy_sale.bumon7_add_attr1 AS "部門7追加属性1"
	,wt74itemstrategy_sale.bumon7_add_attr2 AS "部門7追加属性2"
	,wt74itemstrategy_sale.bumon7_add_attr3 AS "部門7追加属性3"
	,wt74itemstrategy_sale.bumon7_add_attr4 AS "部門7追加属性4"
	,wt74itemstrategy_sale.bumon7_add_attr5 AS "部門7追加属性5"
	,wt74itemstrategy_sale.bumon7_add_attr6 AS "部門7追加属性6"
	,wt74itemstrategy_sale.bumon7_add_attr7 AS "部門7追加属性7"
	,wt74itemstrategy_sale.bumon7_add_attr8 AS "部門7追加属性8"
	,wt74itemstrategy_sale.bumon7_add_attr9 AS "部門7追加属性9"
	,wt74itemstrategy_sale.bumon7_add_attr10 AS "部門7追加属性10"
	,wt74itemstrategy_sale.bumon6_20kisyohingun AS "商品群_20期"
	,wt74itemstrategy_sale.bumon6_20kinaieki1 AS "内訳①_20期"
	,wt74itemstrategy_sale.bumon6_20kinaieki2 AS "内訳②_20期"
	,wt74itemstrategy_sale.bumon6_20kinaieki3 AS "内訳③_20期"
	,wt74itemstrategy_sale.bumon6_zyutensyohinyobi1 AS "重点商品予備1"
	,wt74itemstrategy_sale.bumon6_zyutensyohinyobi2 AS "重点商品予備2"
	,wt74itemstrategy_sale.bumon6_zyutensyohinyobi3 AS "重点商品予備3"
	,wt74itemstrategy_sale.bumon6_zyutensyohinyobi4 AS "重点商品予備4"
	,wt74itemstrategy_sale.bumon6_zyutensyohinyobi5 AS "重点商品予備5"
	,wt74itemstrategy_sale.bumon6_okikaename AS "置き換え_名称"
	,wt74itemstrategy_sale.bumon6_zukyuyosoku1 AS "需給予測1"
	,wt74itemstrategy_sale.bumon6_zukyuyosoku2 AS "需給予測2"
	,wt74itemstrategy_sale.bumon6_zukyuyosoku3 AS "需給予測3"
FROM wt74itemstrategy_sale
)

select * from transformed