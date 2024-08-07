WITH kr_new_stage_point
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.kr_new_stage_point
    ),
dm_kesai_mart_dly_general
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.dm_kesai_mart_dly_general
    ),
tm13item_qv
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.tm13item_qv
    ),
cim08shkos_bunkai_qv
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cim08shkos_bunkai_qv
    ),
cim01kokya
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cim01kokya
    ),
edw_mds_jp_dcl_product_master
AS (
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.edw_mds_jp_dcl_product_master
    ),
item_bunkai
AS (
    SELECT itemcode,
        itemname,
        itemcname,
        tanka,
        chubuncode,
        chubunname,
        chubuncname,
        daibuncode,
        daibunname,
        daibuncname,
        daidaibuncode,
        daidaibunname,
        daidaibuncname,
        bunkai_itemcode,
        bunkai_itemname,
        bunkai_itemcname,
        bunkai_tanka,
        bunkai_kossu,
        bunkai_kosritu
    FROM cim08shkos_bunkai_qv
    ),
item_attr
AS (
    SELECT itemcode AS mds_itemcode,
        itemname AS mds_itemname,
        attr01,
        attr02,
        attr03,
        attr04,
        attr05,
        attr06,
        attr07,
        attr08,
        attr09,
        attr10
    FROM edw_mds_jp_dcl_product_master
    ),
i13
AS (
    SELECT itemcode AS i13_itemcode,
        settanpinsetkbn,
        teikikeiyaku
    FROM tm13item_qv
    ),
ls
AS (
    SELECT yyyymm AS stage_ym,
        kokyano,
        stage
    FROM kr_new_stage_point a
    WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT MAX(yyyymm) AS max_ym
                FROM kr_new_stage_point
                ) stage_max_ym
            WHERE a.yyyymm = stage_max_ym.max_ym
            )
    ),
final
AS (
    SELECT dly.kokyano AS customer_id,
        dly.saleno_key,
        dly.saleno,
        dly.order_dt,
        dly.ship_dt,
        dly.channel AS channel,
        dly.juchkbn,
        dly.h_o_item_code AS kesai_itemcode,
        dly.h_o_item_name AS setitemnm,
        dly.h_o_suryo AS qty,
        TO_DATE(CAST(c.birthday AS VARCHAR), 'YYYYMMDD') AS "誕生日",
        c.rank AS "顧客現在ランク",
        c.seibetukbn AS "性別コード",
        c.carrername AS "職業",
        dly.h_o_item_code AS itemcode,
        item_bunkai.itemname,
        item_bunkai.itemcname,
        item_bunkai.tanka,
        item_bunkai.chubuncode,
        item_bunkai.chubunname,
        item_bunkai.chubuncname,
        item_bunkai.daibuncode,
        item_bunkai.daibunname,
        item_bunkai.daibuncname,
        item_bunkai.daidaibuncode,
        item_bunkai.daidaibunname,
        item_bunkai.daidaibuncname,
        dly.h_item_code AS bunkai_itemcode,
        item_bunkai.bunkai_itemname,
        item_bunkai.bunkai_itemcname,
        item_bunkai.bunkai_tanka,
        item_bunkai.bunkai_kossu,
        item_bunkai.bunkai_kosritu,
        dly.h_item_code AS kose_itemcode,
        COALESCE(dly.z_o_item_code, dly.h_item_code) AS koseiocode,
        COALESCE(dly.z_item_code, dly.h_item_code) AS kosecode,
        dly.z_bun_suryo AS suryo,
        dly.z_koseritsu AS koseritsu,
        item_attr.mds_itemcode,
        item_attr.mds_itemname,
        item_attr.attr01,
        item_attr.attr02,
        item_attr.attr03,
        item_attr.attr04,
        item_attr.attr05,
        item_attr.attr06,
        item_attr.attr07,
        item_attr.attr08,
        item_attr.attr09,
        item_attr.attr10,
        i13.i13_itemcode,
        i13.settanpinsetkbn,
        dly.teikikeiyaku AS teikikeiyaku,
        dly.f,
        COALESCE(ls.stage, 'レギュラー') AS latest_stage,
        ls.stage_ym,
        dly.anbun_amount_tax110_ex AS sales_amt,
        dly.y_order_f AS y_order_f,
        dly.y_ship_f AS y_ship_f
    FROM dm_kesai_mart_dly_general dly
    LEFT JOIN cim01kokya c ON rtrim(dly.kokyano) = rtrim(c.kokyano)
    LEFT JOIN item_bunkai ON rtrim(dly.h_o_item_code) = rtrim(item_bunkai.itemcode)
        AND rtrim(dly.h_item_code) = rtrim(item_bunkai.bunkai_itemcode)
    LEFT JOIN item_attr ON rtrim(dly.z_item_code) = rtrim(item_attr.mds_itemcode)
    LEFT JOIN i13 ON rtrim(dly.h_o_item_code) = rtrim(i13.i13_itemcode)
    LEFT JOIN ls ON rtrim(dly.kokyano) = rtrim(ls.kokyano)
    WHERE dly.order_dt >= TO_DATE('20210101', 'YYYYMMDD')
        AND dly.juchkbn IN ('0', '1', '2')
        AND dly.meisainukikingaku <> 0
    )
SELECT *
FROM final