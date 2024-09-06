with ITEM_JIZEN_BUNKAI_W07 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w07') }}
),

ITEM_JIZEN_BUNKAI_W06 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

trns as
(	
    SELECT T.ITEMCODE AS ITEMCODE,
        SUM(T.KOSERITSU) AS KOSERITSUKEI,
        1 - SUM(T.KOSERITSU) AS SA
    FROM ITEM_JIZEN_BUNKAI_W06 T
    WHERE T.KOSERITSU <> 0
    GROUP BY T.ITEMCODE
    HAVING SUM(T.KOSERITSU) <> 1
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSERITSUKEI::NUMBER(16,8) as KOSERITSUKEI,
        SA::NUMBER(16,8) as SA
    from trns
)

select * from final