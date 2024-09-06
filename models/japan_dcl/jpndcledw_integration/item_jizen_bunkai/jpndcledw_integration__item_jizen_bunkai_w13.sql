with ITEM_JIZEN_BUNKAI_W7 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w7') }}
),

ITEM_JIZEN_BUNKAI_W91 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w91') }}
),

ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

ITEM_JIZEN_BUNKAI_W11 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }}
),

trns as
(	
    SELECT BOM.ITEMCODE AS ITEMCODE,
        BOM.KOSECODE AS KOSECODE,
        BOM.SURYO AS SURYO
    FROM ITEM_JIZEN_BUNKAI_W3 BOM
    WHERE (
            BOM.KOSECODE LIKE '0081%'
            OR BOM.KOSECODE LIKE '0086%'
            )
        AND BOM.ITEMCODE NOT IN (
            SELECT ITEMCODE
            FROM ITEM_JIZEN_BUNKAI_W11
            WHERE SA < 0
            )
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
        KOSECODE::VARCHAR(40) as KOSECODE,
        SURYO::NUMBER(38,4) as SURYO
    from trns
)

select * from final