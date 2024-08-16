WITH dm_kesai_mart_dly_general
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__dm_kesai_mart_dly_general') }}
    ),
cld_m
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cld_m') }}
    ),
cim03item_zaiko
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim03item_zaiko') }}
    ),
edw_mds_jp_dcl_product_master
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__edw_mds_jp_dcl_product_master') }}
    ),
cim24itbun
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim24itbun') }}
    ),
cim03item_hanbai
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__cim03item_hanbai') }}
    ),
tm67juch_nm
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__tm67juch_nm') }}
    ),
zaiko
AS (
    SELECT z.itemcode,
        z.itemkbnname,
        z.bunruicode3_nm,
        z.bunruicode5,
        mds.attr01 AS bumon7_add_attr1,
        mds.attr02 AS bumon7_add_attr2,
        mds.attr03 AS bumon7_add_attr3,
        mds.attr04 AS bumon7_add_attr4,
        mds.attr05 AS bumon7_add_attr5,
        mds.attr06 AS bumon7_add_attr6,
        mds.attr07 AS bumon7_add_attr7,
        mds.attr08 AS bumon7_add_attr8,
        mds.attr09 AS bumon7_add_attr9,
        mds.attr10 AS bumon7_add_attr10
    FROM (
        cim03item_zaiko z LEFT JOIN edw_mds_jp_dcl_product_master mds ON (((z.itemcode)::TEXT = (mds.itemcode)::TEXT))
        )
    ),
final
AS (
    SELECT base.saleno::VARCHAR(63) AS SALENO,
        base.ship_dt::DATE AS "出荷年月日（通常カレンダー基準",
        cld1.ymd_dt::TIMESTAMP_NTZ(9) AS "j&j日付",
        cld1.month_445::VARCHAR(256) AS SHIP_JNJ_MONTH_VIEW,
        cld1.month_445_nm::VARCHAR(256) AS SHIP_JNJ_MONTH_NAME,
        cld1.mweek_445::VARCHAR(256) AS SHIPMENT_WEEK_JNJ,
        cld1.week_ms::VARCHAR(256) AS SHIPMENT_WEEK_JNJ_NUMBER,
        cld1.ymonth_445::VARCHAR(256) AS SHIP_JNJ_MONTH_YEAR,
        dateadd('day', 2, (base.ship_dt)::TIMESTAMP without TIME zone)::DATE AS "着荷日年月日（通常カレンダー基準",
        cld2.month_445::VARCHAR(256) AS DELIVERY_JNJ_MONTH_VIEW,
        cld2.month_445_nm::VARCHAR(256) AS DELIVERY_JNJ_MONTH_NAME,
        cld2.mweek_445::VARCHAR(256) AS DELIVERY_WEEK_JNJ,
        cld2.week_ms::VARCHAR(256) AS DELIVERY_WEEK_JNJ_NUMBER,
        cld2.ymonth_445::VARCHAR(256) AS DELIVERY_JNJ_MONTH_YEAR,
        CASE 
            WHEN ((base.channel)::TEXT = ('通販'::CHARACTER VARYING)::TEXT)
                THEN 'Tsuhan Call'::CHARACTER VARYING
            WHEN ((base.channel)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                THEN 'Tsuhan WEB'::CHARACTER VARYING
            WHEN ((base.channel)::TEXT = ('その他'::CHARACTER VARYING)::TEXT)
                THEN 'Tsuhan WEB'::CHARACTER VARYING
            ELSE base.channel
            END::VARCHAR(20) AS "チャネル",
        base.tokuiname::VARCHAR(346) AS "得意先",
        base.h_o_item_cname::VARCHAR(240) AS "販売商品",
        base.meisaikbn::VARCHAR(36) AS "販売商品区分",
        (((base.h_item_code)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (COALESCE(hanbai.itemname, 'noname'::CHARACTER VARYING))::TEXT)::VARCHAR(16777216) AS "商品構成",
        base.z_o_item_code::VARCHAR(45) AS "在庫商品コード(事前)",
        base.z_item_code::VARCHAR(45) AS "在庫商品コード",
        base.juchkbn::VARCHAR(3) AS "受注区分コード",
        juch_nm.cname::VARCHAR(16777216) AS "受注区分名",
        trunc((base.gts + (0.9)::DOUBLE PRECISION))::FLOAT AS GTS,
        base.gts_qty::FLOAT AS GTS_QTY,
        trunc((base.ciw_return - (0.9)::DOUBLE PRECISION))::FLOAT AS CIW_RETURN,
        base.ciw_return_qty::FLOAT AS CIW_RETURN_QTY,
        trunc((base.ciw_discount + (0.9)::DOUBLE PRECISION))::FLOAT AS CIW_DISCOUNT,
        trunc((base.ciw_point + (0.9)::DOUBLE PRECISION))::FLOAT AS CIW_POINT,
        (((trunc((base.gts + (0.9)::DOUBLE PRECISION)) + trunc((base.ciw_return - (0.9)::DOUBLE PRECISION))) - trunc((base.ciw_discount + (0.9)::DOUBLE PRECISION))) - trunc((base.ciw_point + (0.9)::DOUBLE PRECISION)))::FLOAT AS NTS,
        base.storename::VARCHAR(131) AS "店舗名",
        zaiko.itemkbnname::VARCHAR(100) AS "品目分類値コード1",
        ci.itbunname::VARCHAR(100) AS "品目分類値コード2",
        zaiko.bunruicode3_nm::VARCHAR(100) AS "品目分類値コード3",
        NULL::VARCHAR(16777216) AS "品目グループ名",
        zaiko.bumon7_add_attr1::VARCHAR(200) AS "部門7追加属性1",
        zaiko.bumon7_add_attr2::VARCHAR(200) AS "部門7追加属性2",
        zaiko.bumon7_add_attr3::VARCHAR(200) AS "部門7追加属性3",
        zaiko.bumon7_add_attr4::VARCHAR(200) AS "部門7追加属性4",
        zaiko.bumon7_add_attr5::VARCHAR(200) AS "部門7追加属性5",
        zaiko.bumon7_add_attr6::VARCHAR(200) AS "部門7追加属性6",
        zaiko.bumon7_add_attr7::VARCHAR(200) AS "部門7追加属性7",
        zaiko.bumon7_add_attr8::VARCHAR(200) AS "部門7追加属性8",
        zaiko.bumon7_add_attr9::VARCHAR(200) AS "部門7追加属性9",
        zaiko.bumon7_add_attr10::VARCHAR(200) AS "部門7追加属性10"
    FROM dm_kesai_mart_dly_general base
    LEFT JOIN cld_m cld1 ON (((base.ship_dt)::TIMESTAMP without TIME zone = cld1.ymd_dt))
    LEFT JOIN cld_m cld2 ON ((dateadd('day', 2, (base.ship_dt)::TIMESTAMP without TIME zone))::TIMESTAMP without TIME zone = cld2.ymd_dt)
    LEFT JOIN zaiko ON (((base.z_item_code)::TEXT = (zaiko.itemcode)::TEXT))
    LEFT JOIN cim24itbun ci ON (
            (
                ((ci.itbunshcode)::TEXT = ('5'::CHARACTER VARYING)::TEXT)
                AND ((zaiko.bunruicode5)::TEXT = (ci.itbuncode)::TEXT)
                )
            )
    LEFT JOIN cim03item_hanbai hanbai ON (((base.h_item_code)::TEXT = (hanbai.itemcode)::TEXT))
    LEFT JOIN tm67juch_nm juch_nm ON (((base.juchkbn)::TEXT = (juch_nm.code)::TEXT))
    WHERE (
            (
                (
                    ((base.meisaikbn)::TEXT = ('商品'::CHARACTER VARYING)::TEXT)
                    OR ((base.meisaikbn)::TEXT = ('ポイント交換商品'::CHARACTER VARYING)::TEXT)
                    )
                AND (
                    (
                        ((base.channel)::TEXT = ('通販'::CHARACTER VARYING)::TEXT)
                        OR ((base.channel)::TEXT = ('Web'::CHARACTER VARYING)::TEXT)
                        )
                    OR ((base.channel)::TEXT = ('その他'::CHARACTER VARYING)::TEXT)
                    )
                )
            AND ((base.z_item_code)::TEXT ! LIKE ('999000%'::CHARACTER VARYING)::TEXT)
            )
    )
SELECT *
FROM final
