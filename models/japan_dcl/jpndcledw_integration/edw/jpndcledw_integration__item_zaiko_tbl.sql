WITH tbecitem
AS (
    SELECT *
    FROM dev_dna_core.snapjpdclitg_integration.tbecitem
    ),
item_sap_v
AS (
    select * from dev_dna_core.snapjpdcledw_integration.item_sap_v
    ),
item_bunrval_v
AS (
    select * from dev_dna_core.snapjpdcledw_integration.item_bunrval_v
    ),
zaiko_shohin_attr
AS (
    select * from dev_dna_core.snapjpdcledw_integration.zaiko_shohin_attr
    ),
cim03item_zaiko_ikou_kizuna
AS (
      select * from dev_dna_core.snapjpdcledw_integration.cim03item_zaiko_ikou_kizuna
    ),
c_tbecprivilegemst
AS (
    SELECT *
    FROM dev_dna_core.snapjpdclitg_integration.c_tbecprivilegemst
    ),

ct1
AS (
    SELECT t.dsitemid AS itemcode,
        nvl(t.dsitemname, ' ') AS itemname,
        cast(t.dsitemid AS INT) AS diid,
        nvl(t.dsoption003, ' ') AS itemnamer,
        nvl(isv.jancode, ' ') AS jancode,
        nvl(isv.itemcode_sap, ' ') AS itemcode_sap,
        nvl(t.diitemdivflg, ' ') AS diitemdivflg,
        nvl(ibv1.hin_bunr_val_nms, ' ') AS itemkbnname,
        nvl(cast(t.diitemsalescost AS INTEGER), 0::NUMERIC::NUMERIC(18, 0)) AS tanka,
        nvl(ibv2.j_kaisou_hinmk_bunr_cd, '0000') AS bunruicode2,
        nvl(t.dsoption023, '0000') AS bunruicode3,
        ibv2.hin_bunr_val_nms AS bunruicode3_nm,
        NULL AS bunruicode4,
        nvl(t.dsoption021, '0000') AS bunruicode5,
        TO_NUMBER(REPLACE(TO_CHAR(DATE (t.dsprep), 'YYYY-MM-DD'), '-', '')) AS insertdate,
        t.dsoption001 AS dsoption001,
        t.dsoption002 AS dsoption002,
        NULL AS haiban_hin_cd,
        NULL AS hin_katashiki,
        'ZAIKO' AS syutoku_kbn,
        nvl(isv.bar_cd2, ' ') AS bar_cd2,
        zsa.bumon7_add_attr1 AS bumon7_add_attr1,
        zsa.bumon7_add_attr2 AS bumon7_add_attr2,
        zsa.bumon7_add_attr3 AS bumon7_add_attr3,
        zsa.bumon7_add_attr4 AS bumon7_add_attr4,
        zsa.bumon7_add_attr5 AS bumon7_add_attr5,
        zsa.bumon7_add_attr6 AS bumon7_add_attr6,
        zsa.bumon7_add_attr7 AS bumon7_add_attr7,
        zsa.bumon7_add_attr8 AS bumon7_add_attr8,
        zsa.bumon7_add_attr9 AS bumon7_add_attr9,
        zsa.bumon7_add_attr10 AS bumon7_add_attr10,
        1 AS marker
    FROM tbecitem t
    INNER JOIN item_sap_v isv ON isv.itemcode = t.dsitemid
    LEFT JOIN item_bunrval_v ibv1 ON ibv1.hin_bunr_val_cd = t.diitemdivflg
        AND ibv1.hin_bunr_taik_id = '1'
        AND ibv1.hin_bunr_kaisou_kbn = '3'
    LEFT JOIN item_bunrval_v ibv2 ON ibv2.hin_bunr_val_cd = t.dsoption023
        AND ibv2.hin_bunr_taik_id = '3'
        AND ibv2.hin_bunr_kaisou_kbn = '3'
    LEFT JOIN zaiko_shohin_attr zsa ON t.dsitemid = zsa.shohin_code
    WHERE t.dsoption001 = '在庫商品'
    ),
ct2
AS (
    SELECT czk.itemcode AS itemcode,
        czk.itemname AS itemname,
        cast(czk.itemcode AS INTEGER) AS diid,
        czk.itemnamer AS itemnamer,
        czk.jancode AS jancode,
        NULL AS itemcode_sap,
        czk.itemkbn AS diitemdivflg,
        czk.itemkbnname AS itemkbnname,
        cast(czk.tanka AS INTEGER) AS tanka,
        czk.bunruicode2 AS bunruicode2,
        czk.bunruicode3 AS bunruicode3,
        czk.bunruicode3_nm AS bunruicode3_nm,
        czk.bunruicode4 AS bunruicode4,
        czk.bunruicode5 AS bunruicode5,
        TO_NUMBER(TO_CHAR(DATE_TRUNC('DAY', TO_DATE(TO_CHAR(czk.insertdate), 'YYYYMMDD')), 'YYYYMMDD')) AS insertdate,
        czk.dsoption001 AS dsoption001,
        czk.dsoption002 AS dsoption002,
        czk.haiban_hin_cd AS haiban_hin_cd,
        czk.hin_katashiki AS hin_katashiki,
        czk.syutoku_kbn AS syutoku_kbn,
        czk.bar_cd2 AS bar_cd2,
        zsa.bumon7_add_attr1 AS bumon7_add_attr1,
        zsa.bumon7_add_attr2 AS bumon7_add_attr2,
        zsa.bumon7_add_attr3 AS bumon7_add_attr3,
        zsa.bumon7_add_attr4 AS bumon7_add_attr4,
        zsa.bumon7_add_attr5 AS bumon7_add_attr5,
        zsa.bumon7_add_attr6 AS bumon7_add_attr6,
        zsa.bumon7_add_attr7 AS bumon7_add_attr7,
        zsa.bumon7_add_attr8 AS bumon7_add_attr8,
        zsa.bumon7_add_attr9 AS bumon7_add_attr9,
        zsa.bumon7_add_attr10 AS bumon7_add_attr10,
        2 AS marker
    FROM cim03item_zaiko_ikou_kizuna czk
    LEFT JOIN zaiko_shohin_attr zsa ON czk.itemcode = zsa.shohin_code
    WHERE NOT EXISTS (
            SELECT isv2.itemcode
            FROM item_sap_v isv2
            WHERE czk.itemcode = isv2.itemcode
            )
    ),

ct3
AS (
    SELECT cast(ct.c_diprivilegeid AS VARCHAR) AS itemcode,
        ct.c_dsprivilegename AS itemname,
        cast(ct.c_diprivilegeid AS INT) AS diid,
        ct.c_dsprivilegename AS itemnamer,
        ' ' AS jancode,
        NULL AS itemcode_sap,
        '82' AS itemkbn,
        '特典' AS itemkbnname,
        0 AS tanka,
        '98503' AS bunruicode2,
        '98503' AS bunruicode3,
        NULL AS bunruicode3_nm,
        NULL AS bunruicode4,
        '73' AS bunruicode5,
        20170331 AS insertdate,
        NULL AS dsoption001,
        NULL AS dsoption002,
        NULL AS haiban_hin_cd,
        '非分解' AS hin_katashiki,
        'MANUAL' AS syutoku_kbn,
        NULL AS bar_cd2,
        NULL AS bumon7_add_attr1,
        NULL AS bumon7_add_attr2,
        NULL AS bumon7_add_attr3,
        NULL AS bumon7_add_attr4,
        NULL AS bumon7_add_attr5,
        NULL AS bumon7_add_attr6,
        NULL AS bumon7_add_attr7,
        NULL AS bumon7_add_attr8,
        NULL AS bumon7_add_attr9,
        NULL AS bumon7_add_attr10,
        3 AS marker
    FROM c_tbecprivilegemst ct
    ),
ct4
AS (
    SELECT 'X000000001',
        '利用ポイント数(交換以外)',
        0,
        '利用ポイント数(交換以外)',
        ' ',
        NULL,
        '81',
        'ポイント利用(交換以外)',
        0,
        '98501',
        '98501',
        NULL,
        NULL,
        '71',
        20170331,
        NULL,
        NULL,
        NULL,
        '非分解',
        'MANUAL',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        4
    ),
ct5
AS (
    SELECT 'X000000002',
        '利用ポイント数(交換)',
        0,
        '利用ポイント数(交換)',
        ' ',
        NULL,
        '83',
        'ポイント利用(交換)',
        0,
        '98502',
        '98502',
        NULL,
        NULL,
        '72',
        20170331,
        NULL,
        NULL,
        NULL,
        '非分解',
        'MANUAL',
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        NULL,
        4
    ),
transformed
AS (
    SELECT *
    FROM ct1
    
    UNION
    
    SELECT *
    FROM ct2
    
    UNION
    
    SELECT *
    FROM ct3
    
    UNION
    
    SELECT *
    FROM ct4
    
    UNION
    
    SELECT *
    FROM ct5
    )
    ,
final
AS (
    SELECT itemcode::VARCHAR(30) AS z_itemcode,
        itemname::VARCHAR(400) AS z_itemname,
        diid::number(38, 0) AS diid,
        itemnamer::VARCHAR(200) AS itemnamer,
        jancode::VARCHAR(60) AS jancode,
        itemcode_sap::VARCHAR(40) AS itemcode_sap,
        diitemdivflg::VARCHAR(6) AS itemkbn,
        itemkbnname::VARCHAR(100) AS itemkbnname,
        tanka::number(22, 0) AS tanka,
        bunruicode2::VARCHAR(7) AS bunruicode2,
        bunruicode3::VARCHAR(7) AS bunruicode3,
        bunruicode3_nm::VARCHAR(100) AS bunruicode3_nm,
        bunruicode4::VARCHAR(7) AS bunruicode4,
        bunruicode5::VARCHAR(7) AS bunruicode5,
        insertdate::number(10, 0) AS insertdate,
        dsoption001::VARCHAR(12) AS dsoption001,
        dsoption002::VARCHAR(21) AS dsoption002,
        haiban_hin_cd::VARCHAR(30) AS haiban_hin_cd,
        hin_katashiki::VARCHAR(45) AS hin_katashiki,
        syutoku_kbn::VARCHAR(9) AS syutoku_kbn,
        bar_cd2::VARCHAR(60) AS bar_cd2,
        bumon7_add_attr1::VARCHAR(200) AS bumon7_add_attr1,
        bumon7_add_attr2::VARCHAR(200) AS bumon7_add_attr2,
        bumon7_add_attr3::VARCHAR(200) AS bumon7_add_attr3,
        bumon7_add_attr4::VARCHAR(200) AS bumon7_add_attr4,
        bumon7_add_attr5::VARCHAR(200) AS bumon7_add_attr5,
        bumon7_add_attr6::VARCHAR(200) AS bumon7_add_attr6,
        bumon7_add_attr7::VARCHAR(200) AS bumon7_add_attr7,
        bumon7_add_attr8::VARCHAR(200) AS bumon7_add_attr8,
        bumon7_add_attr9::VARCHAR(200) AS bumon7_add_attr9,
        bumon7_add_attr10::VARCHAR(200) AS bumon7_add_attr10,
        marker::number(38, 0) AS marker,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        NULL::VARCHAR(100) AS inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        NULL::VARCHAR(100) AS updated_by
    FROM transformed
    )
SELECT *
FROM final