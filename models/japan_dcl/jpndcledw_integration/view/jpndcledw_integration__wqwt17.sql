WITH wt17itemstrategy_sale AS
(
    SELECT * FROM dev_dna_core.snapjpdcledw_integration.wt17itemstrategy_sale
),

final AS
(
    SELECT wt17itemstrategy_sale.shukadate AS "出荷年月日",
        wt17itemstrategy_sale.channel AS "チャネル",
        wt17itemstrategy_sale.konyuchubuncode AS "通信経路大区分",
        wt17itemstrategy_sale.hanbaiitem AS "販売商品",
        wt17itemstrategy_sale.kouseiitem AS "商品構成",
        wt17itemstrategy_sale.kuwake1 AS "区分け1",
        wt17itemstrategy_sale.kuwake2 AS "区分け2",
        wt17itemstrategy_sale.juchkbn AS "受注区分コード",
        wt17itemstrategy_sale.juchkbncname AS "c受注区分名",
        wt17itemstrategy_sale.suryo AS "数量",
        wt17itemstrategy_sale.hensu AS "返品数量",
        wt17itemstrategy_sale.urisuryo AS "純売上数量",
        wt17itemstrategy_sale.anbunmaegaku_tyouseimae AS "按分前金額_調整前",
        wt17itemstrategy_sale.meisainukikingaku_tyouseimae AS "明細金額_調整前",
        wt17itemstrategy_sale.anbunmaegaku_tyouseigo AS "按分前金額_調整後",
        wt17itemstrategy_sale.meisainukikingaku_tyouseigo AS "明細金額_調整後",
        wt17itemstrategy_sale.anbunmeisainukikingaku AS "伝票値引按分後売上金額",
        wt17itemstrategy_sale.channelname AS "チャネル名",
        wt17itemstrategy_sale.kuwakename1 AS "区分け1名",
        wt17itemstrategy_sale.kuwakename2 AS "区分け2名",
        wt17itemstrategy_sale.henreasoncode AS "返品理由コード",
        wt17itemstrategy_sale.henreasonname AS "返品理由",
        wt17itemstrategy_sale.kubun2 AS "区分2",
        wt17itemstrategy_sale.syohingun AS "商品群",
        wt17itemstrategy_sale.jyuutenitem AS "重点商品",
        wt17itemstrategy_sale.brandno AS "ブランドno",
        wt17itemstrategy_sale.brand AS "ブランド",
        wt17itemstrategy_sale.categoryno AS "カテゴリno",
        wt17itemstrategy_sale.category AS "カテゴリ",
        wt17itemstrategy_sale.LINENO AS "ラインno",
        wt17itemstrategy_sale.line AS "ライン",
        wt17itemstrategy_sale.itemcategoryno AS "商品カテゴリno",
        wt17itemstrategy_sale.itemcategory AS "商品カテゴリ",
        wt17itemstrategy_sale.kubun2_old AS "旧区分"
    FROM wt17itemstrategy_sale
)

SELECT * FROM final