WITH odr_returnitem_detail
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__odr_returnitem_detail') }}
    ),
cim03item_hanbai
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__cim03item_hanbai') }}
    ),
odr_return_header
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__odr_return_header') }}
    ),
odr_receive_header
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__odr_receive_header') }}
    ),
tbecordermeisai
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__tbecordermeisai') }}
    ),
c_tbeckesai
AS (
    SELECT *
    FROM {{ref('jpndclitg_integration__c_tbeckesai') }}
    ),
final
AS (
    SELECT a.odrreturnno AS "返品no",
        a.odrreturnitemseqno AS "返品明細no",
        to_char(a.returndate, ('YYYY/MM/DD'::CHARACTER VARYING)::TEXT) AS "返品日",
        a.itemcode AS "商品",
        b.itemname AS "商品名",
        a.prequantity AS "数量",
        a.priceinctax AS "単価",
        (a.prequantity * a.priceinctax) AS "金額",
        d.transcode AS "運送会社コード",
        a.cancelflag AS "キャンセルフラグ",
        a.deleteflag AS "削除フラグ",
        a.returntype AS "返品区分",
        to_char(a.returndate, ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS "抽出日付",
        d.odrreceiveno AS "受注no",
        f.divouchercode AS "送り状no"
    FROM odr_returnitem_detail a
    JOIN cim03item_hanbai b ON (((b.itemcode)::TEXT = (a.itemcode)::TEXT))
    JOIN odr_return_header c ON ((c.odrreturnno = a.odrreturnno))
    JOIN odr_receive_header d ON ((d.odrreceiveno = c.odrreceiveno))
    JOIN tbecordermeisai e ON ((e.dimeisaiid = a.returnsourceno))
    JOIN c_tbeckesai f ON ((f.c_dikesaiid = e.c_dikesaiid))
    WHERE (a.returntype = (8)::BIGINT)
    ORDER BY a.odrreturnno,
        a.odrreturnitemseqno
    )
SELECT * FROM final