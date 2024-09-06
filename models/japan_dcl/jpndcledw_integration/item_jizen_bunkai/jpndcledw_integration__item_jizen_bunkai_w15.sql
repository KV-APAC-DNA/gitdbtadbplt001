with ITEM_JIZEN_BUNKAI_W13 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w13') }}
),

ITEM_JIZEN_BUNKAI_W14 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w14') }}
),

ITEM_JIZEN_BUNKAI_W17 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w17') }}
),

trns as
(	
    SELECT BOM.ITEMCODE AS ITEMCODE,
        W10.KOSECODE AS KOSECODE,
        W10.SURYO AS SURYO,
        BOM.KOSERITSUKEI AS KOSERITSUKEI,
        BOM.SA AS SA,
        W8.KOSECODE_CNT AS KOSECODE_CNT
    FROM ITEM_JIZEN_BUNKAI_W17 BOM
    INNER JOIN ITEM_JIZEN_BUNKAI_W13 W10 ON BOM.ITEMCODE = W10.ITEMCODE
    INNER JOIN ITEM_JIZEN_BUNKAI_W14 W8 ON BOM.ITEMCODE = W8.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSECODE::VARCHAR(20) as KOSECODE,
        SURYO::NUMBER(16,8) as SURYO,
        KOSERITSUKEI::NUMBER(16,8) as KOSERITSUKEI,
        SA::NUMBER(16,8) as SA,
        KOSECODE_CNT::NUMBER(16,8) as KOSECODE_CNT
    from trns
)

select * from final