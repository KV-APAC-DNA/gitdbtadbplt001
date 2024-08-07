WITH wt36aspac_sum_customer_item AS
(
   SELECT * FROM dev_dna_core.snapjpdcledw_integration.wt36aspac_sum_customer_item
),

final AS
(
    SELECT wt36aspac_sum_customer_item.sum_name AS "集計用名称",
        wt36aspac_sum_customer_item.itemcode AS "商品コード",
        wt36aspac_sum_customer_item.itemname AS "商品名",
        wt36aspac_sum_customer_item.itemkbn AS "品目分類値コード1",
        wt36aspac_sum_customer_item.itemkbnname AS "品目分類値名",
        wt36aspac_sum_customer_item.tokuicode AS "得意先コード",
        wt36aspac_sum_customer_item.tokuiname AS "得意先名",
        wt36aspac_sum_customer_item.tokuiname_ryaku AS "得意先名略",
        wt36aspac_sum_customer_item.tokuiname2 AS "得意先名2",
        wt36aspac_sum_customer_item.shimebi AS "対象月次締日",
        wt36aspac_sum_customer_item.suryo AS "数量",
        wt36aspac_sum_customer_item.kingaku AS "金額",
        wt36aspac_sum_customer_item.nohindate AS "納品日",
        wt36aspac_sum_customer_item.shukadate AS "出荷日",
        wt36aspac_sum_customer_item.torikeikbn AS "取引区分",
        wt36aspac_sum_customer_item.salekeijodate AS "売上計上日売上返品計上日"
    FROM wt36aspac_sum_customer_item
)

SELECT * FROM final