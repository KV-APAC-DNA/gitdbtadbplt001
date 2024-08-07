WITH item_hanbai_v AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_hanbai_v
),

item_jizen_bunkai_v AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_jizen_bunkai_v
),

item_pick_bunkai_v AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_pick_bunkai_v
),

item_z_h_hen_v AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_z_h_hen_v
),

item_zaiko_v AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_zaiko_v
),

item_zaiko_tbl AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.item_zaiko_tbl
),

cim24itbun_qv AS
(
    SELECT *
    FROM dev_dna_core.snapjpdcledw_integration.cim24itbun_qv
),

final AS   
(    
    SELECT zai.z_itemcode AS z_item_cd,
        zai.z_itemname AS z_item_nm,
        zai_oya.z_itemcode AS z_o_item_cd,
        zai_oya.z_itemname AS z_o_item_nm,
        z_bun.suryo AS z_bun_suryo,
        z_bun.koseritsu AS z_bun_koseritsu,
        han.h_itemcode AS h_item_cd,
        han.h_itemname AS h_item_nm,
        han_oya.h_itemcode AS h_o_item_cd,
        han_oya.h_itemname AS h_o_item_nm,
        h_bun.bunkai_kossu AS h_bun_suryo,
        h_bun.bunkai_kosritu AS h_bun_koseritsu,
        zai.bunruicode5 AS z_bunrui_cd5,
        itbun_daidai.itbunname AS z_daidaibunname,
        zai.bumon7_add_attr1 AS z_b7_add_attr1,
        zai.bumon7_add_attr2 AS z_b7_add_attr2,
        zai.bumon7_add_attr3 AS z_b7_add_attr3,
        zai.bumon7_add_attr4 AS z_b7_add_attr4,
        zai.bumon7_add_attr5 AS z_b7_add_attr5,
        zai.bumon7_add_attr6 AS z_b7_add_attr6,
        zai.bumon7_add_attr7 AS z_b7_add_attr7,
        zai.bumon7_add_attr8 AS z_b7_add_attr8,
        zai.bumon7_add_attr9 AS z_b7_add_attr9,
        1 AS maker
    FROM item_zaiko_tbl zai 
    LEFT JOIN item_jizen_bunkai_v z_bun 
        ON zai.z_itemcode = z_bun.kosei_cd 
        AND z_bun.bunkaikbn = '1'
    LEFT JOIN item_zaiko_tbl zai_oya 
        ON z_bun.item_cd = zai_oya.z_itemcode
    LEFT JOIN item_z_h_hen_v z_h_hen 
        ON zai.z_itemcode = z_h_hen.z_itemcode
    LEFT JOIN item_hanbai_v han 
        ON z_h_hen.h_itemcode = han.h_itemcode
    LEFT JOIN item_pick_bunkai_v h_bun 
        ON z_h_hen.h_itemcode = h_bun.bunkai_itemcode
    LEFT JOIN item_hanbai_v han_oya 
        ON h_bun.itemcode = han_oya.h_itemcode
    LEFT JOIN cim24itbun_qv itbun_daidai 
        ON zai.bunruicode5 = itbun_daidai.itbuncode 
        AND itbun_daidai.itbunshcode = '5'
        
    UNION ALL
    
    SELECT 'DUMMY' AS z_item_cd,
        'DUMMY' AS z_item_nm,
        'DUMMY' AS z_o_item_cd,
        'DUMMY' AS z_o_item_nm,
        0 AS z_bun_suryo,
        0 AS z_bun_koseritsu,
        CAST(han.h_itemcode AS VARCHAR(60)) AS h_item_cd,
        han.h_itemname AS h_item_nm,
        CAST(han_oya.h_itemcode AS VARCHAR(60)) AS h_o_item_cd,
        han_oya.h_itemname AS h_o_item_nm,
        h_bun.bunkai_kossu AS h_bun_suryo,
        h_bun.bunkai_kosritu AS h_bun_koseritsu,
        'DUMMY' AS z_bunrui_cd5,
        'DUMMY' AS z_daidaibunname,
        'DUMMY' AS z_b7_add_attr1,
        'DUMMY' AS z_b7_add_attr2,
        'DUMMY' AS z_b7_add_attr3,
        'DUMMY' AS z_b7_add_attr4,
        'DUMMY' AS z_b7_add_attr5,
        'DUMMY' AS z_b7_add_attr6,
        'DUMMY' AS z_b7_add_attr7,
        'DUMMY' AS z_b7_add_attr8,
        'DUMMY' AS z_b7_add_attr9,
        2 AS maker
    FROM item_hanbai_v han 
    LEFT JOIN item_pick_bunkai_v h_bun 
        ON han.h_itemcode = h_bun.bunkai_itemcode
    LEFT JOIN item_hanbai_v han_oya 
        ON h_bun.itemcode = han_oya.h_itemcode
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT item_zaiko_v.z_itemcode AS itemcode
            FROM item_zaiko_v
            
            UNION
            
            SELECT item_hanbai_v.h_itemcode AS itemcode
            FROM item_hanbai_v
        ) zai_mart 
        JOIN item_z_h_hen_v hen_mart 
            ON zai_mart.itemcode = hen_mart.z_itemcode
        WHERE han.h_itemcode = hen_mart.h_itemcode
    )
)

SELECT *
FROM final