with ITEM_JIZEN_BUNKAI_W7 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w7') }}
),

ITEM_JIZEN_BUNKAI_WZ as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }}
),

ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

trns as
(	
    SELECT BOM.ITEMCODE AS ITEMCODE,
        BOM.KOSECODE AS KOSECODE,
        (
            CASE 
                WHEN ZAIKO.TANKA = 0
                    THEN CASE 
                            WHEN W7.KOTANKASUM = 0
                                THEN 0
                            ELSE ROUND(BOM.SURYO * ZAIKO2.TANKA / W7.KOTANKASUM::DECIMAL, 8)
                            END
                ELSE ROUND(BOM.SURYO * ZAIKO2.TANKA / ZAIKO.TANKA::DECIMAL, 8)
                END
            ) AS KOSERITSU
    FROM ITEM_JIZEN_BUNKAI_W3 BOM
    LEFT JOIN ITEM_JIZEN_BUNKAI_WZ ZAIKO ON BOM.ITEMCODE = ZAIKO.ITEMCODE
    LEFT JOIN ITEM_JIZEN_BUNKAI_WZ ZAIKO2 ON BOM.KOSECODE = ZAIKO2.ITEMCODE
    LEFT JOIN ITEM_JIZEN_BUNKAI_W7 W7 ON BOM.ITEMCODE = W7.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSECODE::VARCHAR(20) as KOSECODE,
        KOSERITSU::NUMBER(16,8) as KOSERITSU
    from trns
)

select * from final