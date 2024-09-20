with ITEM_JIZEN_BUNKAI_W91 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w91') }}
),

ITEM_JIZEN_BUNKAI_W92 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w92') }}
),

trns as
(	
    SELECT T1.ITEMCODE AS ITEMCODE
    FROM ITEM_JIZEN_BUNKAI_W91 T1,
        ITEM_JIZEN_BUNKAI_W92 T2
    WHERE T1.ITEMCODE = T2.ITEMCODE
        AND T1.CNT <> T2.CNT
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE
    from trns
)

select * from final