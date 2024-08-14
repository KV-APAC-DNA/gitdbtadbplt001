WITH sscmnhin
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration', 'sscmnhin') }}
    ),
sscmnhingrp
AS (
    SELECT *
    FROM {{ source('jpdclitg_integration', 'sscmnhingrp') }}
    ),
cld_m
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'cld_m') }}
    ),
edw_mds_jp_dcl_partner_master
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__edw_mds_jp_dcl_partner_master')}}
    ),
wt74itemstrategy_sale_p_new
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__wt74itemstrategy_sale_p_new')}}
    ),
transformed
AS (
    SELECT A.SHUKANENGETUBI AS "出荷年月日（通常カレンダー基準）" --"出荷年月日"
        ,
        B.YMD_DT AS "J&J日付",
        B.MONTH_445 AS "SHIP_JNJ_MONTH_VIEW",
        B.MONTH_445_NM AS "SHIP_JNJ_MONTH_NAME",
        B.MWEEK_445 AS "SHIPMENT_WEEK_JNJ",
        B.WEEK_MS AS "SHIPMENT_WEEK_JNJ_NUMBER",
        B.YMONTH_445 AS "SHIP_JNJ_MONTH_YEAR" --"出荷年月日（JnJカレンダー基準）"
        ,
        try_to_date(DATEADD(DAY, 2, A.SHUKANENGETUBI)) AS "着荷日年月日（通常カレンダー基準）" --"DELIVERY_DATE"
        ,
        C.MONTH_445 AS "DELIVERY_JNJ_MONTH_VIEW",
        C.MONTH_445_NM AS "DELIVERY_JNJ_MONTH_NAME",
        C.MWEEK_445 AS "DELIVERY_WEEK_JNJ",
        C.WEEK_MS AS "DELIVERY_WEEK_JNJ_NUMBER",
        C.YMONTH_445 AS "DELIVERY_JNJ_MONTH_YEAR" --"着荷年月日（JnJカレンダー基準）"
        ,
        A.CHANNEL AS "チャネル",
        A.KONYUCHUBUNCODE AS "通信経路大区分",
        A.TORIKEIKBN AS "取引形態区分",
        A.TOKUICODE AS "得意先",
        A.HANBAIITEM AS "販売商品",
        A.HANBAIITEM_KUBUN AS "販売商品区分",
        DECODE(NUM, 1, A.HANBAIITEM_SURYO, 0) AS "セット品数量",
        DECODE(NUM, 1, A.HANBAIITEM_GAKU, 0) AS "販売商品定価×セット品数量",
        A.KOUSEIITEM AS "商品構成",
        A.KOSECODE AS "在庫商品コード",
        A.KOUSEIITEM_GAKU AS "商品構成定価×純売上数量",
        A.JUCHKBN AS "受注区分コード",
        A.JUCHKBNCNAME AS "受注区分名",
        ROUND(A.ANBUN_TOKUTEN_GAKU) AS "CIW_Discount" --"クーポン等値引き金額"
        ,
        ROUND(A.SURYO) AS "GTS_Qty" --"数量"
        ,
        ROUND(A.HENSU) AS "CIW_Return_Qty" --"返品数量"
        ,
        ROUND(A.URISURYO) AS "純売上数量",
        ROUND(A.ANBUNMAEGAKU_TYOUSEIMAE) AS "按分前金額_調整前",
        ROUND(A.MEISAINUKIKINGAKU_TYOUSEIMAE) AS "明細金額_調整前",
        ROUND(A.ANBUNMAEGAKU_TYOUSEIGO) AS "按分前金額_調整後",
        ROUND(A.MEISAINUKIKINGAKU_TYOUSEIGO) AS "明細金額_調整後",
        ROUND(A.ANBUNMEISAINUKIKINGAKU) AS "伝票値引按分後売上金額",
        ROUND(A.ANBUN_RIYOPOINT_ZEINUKI) AS "CIW_Point" --"税抜按分利用ポイント"
        ,
        ROUND(A.ANBUN_RIYOPOINT_ZEIKOMI) AS "CIW_Point (inc tax)" --"税込按分利用ポイント"
        ,
        ROUND(A.GTS) AS "GTS",
        ROUND(A.CIW_RETURN) AS "CIW_Return",
        ROUND(A.GTS) + ROUND(A.CIW_RETURN) - ROUND(A.ANBUN_TOKUTEN_GAKU) - ROUND(A.ANBUN_RIYOPOINT_ZEINUKI) AS "NTS",
        A.CHANNEL1 AS "チャネル大区分",
        A.CHANNEL2 AS "チャネル中区分",
        A.CHANNEL3 AS "チャネル小区分",
        A.BMN_HYOUJI_CD AS "売上計上部門コード",
        A.BMN_NMS AS "売上計上部門名",
        A.HENREASONCODE AS "返品理由コード",
        A.HENREASONNAME AS "返品理由",
        A.SOKONAME AS "店舗名",
        A.KUBUN2 AS "区分2",
        A.SYOHINGUN AS "商品群",
        A.JYUUTENITEM AS "重点商品",
        A.ITEM_BUNR_VAL1 AS "品目分類値コード1",
        A.ITEM_BUNR_VAL2 AS "品目分類値コード2",
        A.ITEM_BUNR_VAL3 AS "品目分類値コード3",
        F.HIN_GRP_NMS AS "品目グループ名",
        A.BUMON7_ADD_ATTR1 AS "部門7追加属性1",
        A.BUMON7_ADD_ATTR2 AS "部門7追加属性2",
        A.BUMON7_ADD_ATTR3 AS "部門7追加属性3",
        A.BUMON7_ADD_ATTR4 AS "部門7追加属性4",
        A.BUMON7_ADD_ATTR5 AS "部門7追加属性5",
        A.BUMON7_ADD_ATTR6 AS "部門7追加属性6",
        A.BUMON7_ADD_ATTR7 AS "部門7追加属性7",
        A.BUMON7_ADD_ATTR8 AS "部門7追加属性8",
        A.BUMON7_ADD_ATTR9 AS "部門7追加属性9",
        A.BUMON7_ADD_ATTR10 AS "部門7追加属性10",
        A.BUMON6_20KISYOHINGUN AS "商品群_20期",
        A.BUMON6_20KINAIEKI1 AS "内訳①_20期",
        A.BUMON6_20KINAIEKI2 AS "内訳②_20期",
        A.BUMON6_20KINAIEKI3 AS "内訳③_20期",
        A.BUMON6_ZYUTENSYOHINYOBI1 AS "重点商品予備1",
        A.BUMON6_ZYUTENSYOHINYOBI2 AS "重点商品予備2",
        A.BUMON6_ZYUTENSYOHINYOBI3 AS "重点商品予備3",
        A.BUMON6_ZYUTENSYOHINYOBI4 AS "重点商品予備4",
        A.BUMON6_ZYUTENSYOHINYOBI5 AS "重点商品予備5",
        A.BUMON6_OKIKAENAME AS "置き換え_名称",
        A.BUMON6_ZUKYUYOSOKU1 AS "需給予測1",
        A.BUMON6_ZUKYUYOSOKU2 AS "需給予測2",
        A.BUMON6_ZUKYUYOSOKU3 AS "需給予測3",
        D.column37 AS "得意先区分(MDS)",
        CASE 
            WHEN A.CHANNEL1 = '卸売'
                AND A.CHANNEL2 = '卸売'
                AND A.CHANNEL3 = '職域（特販）'
                THEN D.column37
            WHEN A.CHANNEL1 = '卸売'
                AND A.CHANNEL2 = '卸売'
                AND A.CHANNEL3 = '職域（販売会）'
                THEN 'Wholesale'
            WHEN A.CHANNEL1 = '卸売'
                AND A.CHANNEL2 = '卸売'
                AND A.CHANNEL3 = '代理店'
                THEN D.column37
            WHEN A.CHANNEL1 = '卸売'
                AND A.CHANNEL2 = '卸売'
                AND A.CHANNEL3 = '職域（代理店）'
                THEN 'Wholesale'
            WHEN A.CHANNEL1 = '卸売'
                AND A.CHANNEL2 = 'QVC'
                AND A.CHANNEL3 = 'QVC'
                THEN 'QVC'
            WHEN A.CHANNEL1 = '海外'
                AND A.CHANNEL2 = '海外'
                AND A.CHANNEL3 = '海外免税'
                THEN 'Travel Retail'
            WHEN A.CHANNEL1 = '海外'
                AND A.CHANNEL2 = '海外'
                AND A.CHANNEL3 = '国内免税'
                THEN 'Travel Retail'
            WHEN A.CHANNEL1 = '海外'
                AND A.CHANNEL2 = 'JJ'
                AND A.CHANNEL3 = 'JJ'
                THEN D.column37
            WHEN A.CHANNEL1 = '通信販売'
                AND A.CHANNEL2 = '通販'
                AND A.CHANNEL3 = '社販'
                THEN 'Tsuhan WEB'
            WHEN A.CHANNEL1 = '通信販売'
                AND A.CHANNEL2 = '通販'
                AND A.CHANNEL3 = '通販'
                THEN 'Tsuhan Call'
            WHEN A.CHANNEL1 = '通信販売'
                AND A.CHANNEL2 = '通販'
                AND A.CHANNEL3 = 'VIP'
                THEN 'Wholesale'
            WHEN A.CHANNEL1 = '通信販売'
                AND A.CHANNEL2 = 'WEB'
                AND A.CHANNEL3 = 'WEB'
                THEN 'Tsuhan WEB'
            WHEN A.CHANNEL1 = '対面販売'
                AND A.CHANNEL2 = '店舗'
                AND A.CHANNEL3 = '買取'
                THEN D.column37
            WHEN A.CHANNEL1 = '対面販売'
                AND A.CHANNEL2 = '店舗'
                AND A.CHANNEL3 = '消化'
                THEN 'Store'
            WHEN A.CHANNEL1 = '対面販売'
                AND A.CHANNEL2 = '店舗'
                AND A.CHANNEL3 = 'アウトレット'
                THEN 'Store'
            WHEN A.CHANNEL1 = '対面販売'
                AND A.CHANNEL2 = '店舗'
                AND A.CHANNEL3 = '直営'
                THEN D.column37
            WHEN A.CHANNEL1 = 'その他'
                AND A.CHANNEL2 = 'その他'
                AND A.CHANNEL3 = 'FS'
                THEN 'Tsuhan Call'
            ELSE NULL
            END AS "得意先区分"
    FROM WT74ITEMSTRATEGY_SALE_P_NEW A
    LEFT JOIN CLD_M B ON A.SHUKANENGETUBI = B.YMD_DT
    LEFT JOIN CLD_M C ON try_to_date(dateadd(day, 2, A.SHUKANENGETUBI)) = C.YMD_DT
    LEFT JOIN EDW_MDS_JP_DCL_PARTNER_MASTER D ON A.TOKUICODE_KEY = D.TOKUICODE
    LEFT JOIN SSCMNHIN E ON A.KOSECODE = E.HIN_CD
    LEFT JOIN SSCMNHINGRP F ON E.HIN_GRP_CD = F.HIN_GRP_CD
    ),
final
AS (
    SELECT "出荷年月日（通常カレンダー基準）"::DATE AS "出荷年月日（通常カレンダー基準）",
        "J&J日付"::timestamp_ntz(9) AS "J&J日付",
        ship_jnj_month_view::VARCHAR(256) AS ship_jnj_month_view,
        ship_jnj_month_name::VARCHAR(256) AS ship_jnj_month_name,
        shipment_week_jnj::VARCHAR(256) AS shipment_week_jnj,
        shipment_week_jnj_number::VARCHAR(256) AS shipment_week_jnj_number,
        ship_jnj_month_year::VARCHAR(256) AS ship_jnj_month_year,
        "着荷日年月日（通常カレンダー基準）"::DATE AS "着荷日年月日（通常カレンダー基準）",
        delivery_jnj_month_view::VARCHAR(256) AS delivery_jnj_month_view,
        delivery_jnj_month_name::VARCHAR(256) AS delivery_jnj_month_name,
        delivery_week_jnj::VARCHAR(256) AS delivery_week_jnj,
        delivery_week_jnj_number::VARCHAR(256) AS delivery_week_jnj_number,
        delivery_jnj_month_year::VARCHAR(256) AS delivery_jnj_month_year,
        "チャネル"::VARCHAR(3) AS "チャネル",
        "通信経路大区分"::VARCHAR(60) AS "通信経路大区分",
        "取引形態区分"::VARCHAR(4) AS "取引形態区分",
        "得意先"::VARCHAR(346) AS "得意先",
        "販売商品"::VARCHAR(463) AS "販売商品",
        "販売商品区分"::VARCHAR(6000) AS "販売商品区分",
        "セット品数量"::number(38, 25) AS "セット品数量",
        "販売商品定価×セット品数量"::number(38, 25) AS "販売商品定価×セット品数量",
        "商品構成"::VARCHAR(463) AS "商品構成",
        "在庫商品コード"::VARCHAR(45) AS "在庫商品コード",
        "商品構成定価×純売上数量"::number(38, 25) AS "商品構成定価×純売上数量",
        "受注区分コード"::VARCHAR(4) AS "受注区分コード",
        "受注区分名"::VARCHAR(223) AS "受注区分名",
        "CIW_Discount"::FLOAT AS ciw_discount,
        "GTS_Qty"::number(14, 0) AS gts_qty,
        "CIW_Return_Qty"::number(17, 0) AS ciw_return_qty,
        "純売上数量"::number(14, 0) AS "純売上数量",
        "按分前金額_調整前"::number(38, 0) AS "按分前金額_調整前",
        "明細金額_調整前"::number(31, 0) AS "明細金額_調整前",
        "按分前金額_調整後"::number(37, 0) AS "按分前金額_調整後",
        "明細金額_調整後"::FLOAT AS "明細金額_調整後",
        "伝票値引按分後売上金額"::FLOAT AS "伝票値引按分後売上金額",
        "CIW_Point"::FLOAT AS ciw_point,
        "CIW_Point (inc tax)"::FLOAT AS "ciw_point (inc tax)",
        gts::FLOAT AS gts,
        "CIW_Return"::FLOAT AS ciw_return,
        nts::FLOAT AS nts,
        "チャネル大区分"::VARCHAR(12) AS "チャネル大区分",
        "チャネル中区分"::VARCHAR(9) AS "チャネル中区分",
        "チャネル小区分"::VARCHAR(21) AS "チャネル小区分",
        "売上計上部門コード"::VARCHAR(20) AS "売上計上部門コード",
        "売上計上部門名"::VARCHAR(240) AS "売上計上部門名",
        "返品理由コード"::VARCHAR(6) AS "返品理由コード",
        "返品理由"::VARCHAR(909) AS "返品理由",
        "店舗名"::VARCHAR(131) AS "店舗名",
        "区分2"::VARCHAR(22) AS "区分2",
        "商品群"::VARCHAR(22) AS "商品群",
        "重点商品"::VARCHAR(22) AS "重点商品",
        "品目分類値コード1"::VARCHAR(100) AS "品目分類値コード1",
        "品目分類値コード2"::VARCHAR(100) AS "品目分類値コード2",
        "品目分類値コード3"::VARCHAR(100) AS "品目分類値コード3",
        "品目グループ名"::VARCHAR(20) AS "品目グループ名",
        "部門7追加属性1"::VARCHAR(200) AS "部門7追加属性1",
        "部門7追加属性2"::VARCHAR(200) AS "部門7追加属性2",
        "部門7追加属性3"::VARCHAR(200) AS "部門7追加属性3",
        "部門7追加属性4"::VARCHAR(200) AS "部門7追加属性4",
        "部門7追加属性5"::VARCHAR(200) AS "部門7追加属性5",
        "部門7追加属性6"::VARCHAR(200) AS "部門7追加属性6",
        "部門7追加属性7"::VARCHAR(200) AS "部門7追加属性7",
        "部門7追加属性8"::VARCHAR(200) AS "部門7追加属性8",
        "部門7追加属性9"::VARCHAR(200) AS "部門7追加属性9",
        "部門7追加属性10"::VARCHAR(200) AS "部門7追加属性10",
        "商品群_20期"::VARCHAR(1) AS "商品群_20期",
        "内訳①_20期"::VARCHAR(1) AS "内訳①_20期",
        "内訳②_20期"::VARCHAR(1) AS "内訳②_20期",
        "内訳③_20期"::VARCHAR(1) AS "内訳③_20期",
        "重点商品予備1"::VARCHAR(1) AS "重点商品予備1",
        "重点商品予備2"::VARCHAR(1) AS "重点商品予備2",
        "重点商品予備3"::VARCHAR(1) AS "重点商品予備3",
        "重点商品予備4"::VARCHAR(1) AS "重点商品予備4",
        "重点商品予備5"::VARCHAR(1) AS "重点商品予備5",
        "置き換え_名称"::VARCHAR(1) AS "置き換え_名称",
        "需給予測1"::VARCHAR(1) AS "需給予測1",
        "需給予測2"::VARCHAR(1) AS "需給予測2",
        "需給予測3"::VARCHAR(1) AS "需給予測3",
        "得意先区分(MDS)"::VARCHAR(200) AS "得意先区分(MDS)",
        "得意先区分"::VARCHAR(200) AS "得意先区分"
    FROM transformed
    )
SELECT *
FROM final
