WITH c_tbecprivilegemst
AS (
    select * from {{ ref('jpndclitg_integration__c_tbecprivilegemst') }}
    ),
tbecitem
AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__tbecitem') }}
    ),
item_bunrval_v
AS (
    select * from {{ source('jpndcledw_integration', 'item_bunrval_v') }}
    ),
cim03item_ikou
AS (
     select * from {{ source('jpndcledw_integration', 'cim03item_ikou') }}
    ),
ct1
AS (
    SELECT cast(c_diprivilegeid AS VARCHAR) AS h_itemcode,
        c_dsprivilegename AS h_itemname,
        cast(c_diprivilegeid AS INT) AS diid,
        c_dsprivilegename AS itemnamer,
        '82' AS itemkbn,
        0 AS tanka,
        0 AS tanka_sales,
        '98503' AS bunruicode2,
        '98503' AS bunruicode3,
        NULL AS bunruicode4,
        '73' AS bunruicode5,
        20170331 AS insertdate,
        NULL AS dsoption001,
        '単品' AS dsoption002,
        NULL AS dsoption010,
        NULL AS haiban_hin_cd,
        'MANUAL' AS syutoku_kbn,
        4 AS MARKER
    FROM c_tbecprivilegemst
    ),
ct2
AS (
    SELECT t.dsitemid AS h_itemcode,
        nvl(t.dsitemname, '') AS h_itemname,
        t.diid AS diid,
        nvl(t.dsoption003, '') AS itemnamer,
        nvl(t.diitemdivflg, '00') AS itemkbn,
        nvl(t.diitemsalescost, '0') AS tanka,
        nvl(t.diItemSalesPrc, '0') AS tanka_sales,
        nvl(ibv.j_kaisou_hinmk_bunr_cd, '00000') AS bunruicode2,
        nvl(trim(t.dsoption023), '00000') AS bunruicode3,
        NULL AS bunruicode4,
        nvl(trim(t.dsoption021), '00000') AS bunruicode5,
        TO_NUMBER(REPLACE(TO_CHAR(DATE (t.dsprep), 'YYYY-MM-DD'), '-', '')) AS insertdate,
        trim(t.dsoption001) AS dsoption001,
        trim(t.dsoption002) AS dsoption002,
        trim(t.dsoption010) AS dsoption010,
        NULL AS haiban_hin_cd,
        'NEXT' AS syutoku_kbn,
        1 AS MARKER
    FROM tbecitem t
    LEFT JOIN item_bunrval_v ibv ON t.dsoption023 = ibv.hin_bunr_val_cd
        AND ibv.hin_bunr_taik_id = '3'
        AND ibv.hin_bunr_kaisou_kbn = '3'
    WHERE t.dsoption001 = '販売商品'
        AND t.dielimflg = 0
    ),
ct3
AS (
    SELECT t1.dsitemid AS h_itemcode,
        nvl(t1.dsitemname, '') AS h_itemname,
        t1.diid AS diid,
        nvl(t1.dsoption003, '') AS itemnamer,
        nvl(t1.diitemdivflg, '00') AS itemkbn,
        nvl(t1.diitemsalescost, '0') AS tanka,
        nvl(t1.diItemSalesPrc, '0') AS tanka_sales,
        nvl(ibv1.j_kaisou_hinmk_bunr_cd, '00000') AS bunruicode2,
        nvl(trim(t1.dsoption023), '00000') AS bunruicode3,
        NULL AS bunruicode4,
        nvl(trim(t1.dsoption021), '00000') AS bunruicode5,
        TO_NUMBER(REPLACE(TO_CHAR(DATE (t1.dsprep), 'YYYY-MM-DD'), '-', '')) AS insertdate,
        Trim(t1.dsoption001) AS dsoption001,
        Trim(t1.dsoption002) AS dsoption002,
        Trim(t1.dsoption010) AS dsoption010,
        NULL AS haiban_hin_cd,
        'NEXT' AS syutoku_kbn,
        2 AS MARKER
    FROM tbecitem t1
    LEFT JOIN item_bunrval_v ibv1 ON t1.dsoption023 = ibv1.hin_bunr_val_cd
        AND ibv1.hin_bunr_taik_id = '3'
        AND ibv1.hin_bunr_kaisou_kbn = '3'
    WHERE t1.dsoption001 = '販売商品'
        AND NOT EXISTS (
            SELECT *
            FROM tbecitem t2
            WHERE t1.dsitemid = t2.dsitemid
                AND t2.dielimflg = 0
            )
    ),
ct4
AS (
    SELECT kako.itemcode AS h_itemcode,
        nvl(kako.itemname, '') AS h_itemname,
        CAST(kako.itemcode AS INT) AS diid,
        nvl(kako.itemnamer, '') AS itemnamer,
        nvl(kako.itemkbn, '00') AS itemkbn,
        nvl(kako.tanka, 0) AS tanka,
        nvl(kako.tankanew, 0) AS tanka_sales,
        nvl(kako.bunruicode2, '00000') AS bunruicode2,
        nvl(kako.bunruicode3, '00000') AS bunruicode3,
        nvl(kako.bunruicode4, '00000') AS bunruicode4,
        nvl(kako.bunruicode5, '00000') AS bunruicode5,
        nvl(kako.insertdate, 0) AS insertdate,
        NULL AS dsoption001,
        NULL AS dsoption002,
        NULL AS dsoption010,
        NULL AS haiban_hin_cd,
        'KAKO' AS syutoku_kbn,
        3 AS MARKER
    FROM cim03item_ikou kako
    WHERE NOT EXISTS (
            SELECT *
            FROM tbecitem item
            WHERE item.dsitemid = kako.itemcode
            )
    ),
ct5
AS (
    SELECT 'X000000001',
        '利用ポイント数(交換以外)',
        0,
        '利用ポイント数(交換以外)',
        '81',
        0,
        0,
        '98501',
        '98501',
        NULL,
        '71',
        20170331,
        NULL,
        '単品',
        NULL,
        NULL,
        'MANUAL',
        5
    ),
ct6
AS (
    SELECT 'X000000002',
        'ポイント利用(交換)',
        0,
        'ポイント利用(交換)',
        '83',
        0,
        0,
        '98502',
        '98502',
        NULL,
        '72',
        20170331,
        NULL,
        '単品',
        NULL,
        NULL,
        'MANUAL',
        5
    ),
tr
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
    
    UNION
    
    SELECT *
    FROM ct6
    ),
final
AS (
    SELECT h_itemcode::VARCHAR(45) AS h_itemcode,
        h_itemname::VARCHAR(400) AS h_itemname,
        diid::number(38, 0) AS diid,
        itemnamer::VARCHAR(200) AS itemnamer,
        itemkbn::VARCHAR(6) AS itemkbn,
        tanka::number(22, 0) AS tanka,
        tanka_sales::number(22, 0) AS tanka_sales,
        bunruicode2::VARCHAR(7) AS bunruicode2,
        bunruicode3::VARCHAR(7) AS bunruicode3,
        bunruicode4::VARCHAR(7) AS bunruicode4,
        bunruicode5::VARCHAR(7) AS bunruicode5,
        insertdate::number(10, 0) AS insertdate,
        dsoption001::VARCHAR(12) AS dsoption001,
        dsoption002::VARCHAR(21) AS dsoption002,
        dsoption010::VARCHAR(21) AS dsoption010,
        haiban_hin_cd::VARCHAR(30) AS haiban_hin_cd,
        syutoku_kbn::VARCHAR(6) AS syutoku_kbn,
        marker::number(38, 0) AS marker,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        NULL::VARCHAR(100) AS inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        NULL::VARCHAR(100) AS updated_by
    FROM tr
    )
SELECT *
FROM final