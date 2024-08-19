with ITEM_JIZEN_BUNKAI_W081 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w081') }}
),


ITEM_JIZEN_BUNKAI_WEND as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
),


trns as
(	
    SELECT T1.ITEMCODE AS ITEMCODE,
        T1.KOSECODE AS KOSECODE,
        T1.KOSERITSU AS KOSERITSU,
        T2.KOSERITSUKEI AS KOSERITSUKEI,
        T2.SA AS SA
    FROM ITEM_JIZEN_BUNKAI_WEND T1,
        ITEM_JIZEN_BUNKAI_W081 T2
    WHERE T1.ITEMCODE = T2.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSECODE::VARCHAR(20) as KOSECODE,
        KOSERITSU::NUMBER(16,8) as KOSERITSU,
        KOSERITSUKEI::NUMBER(16,8) as KOSERITSUKEI,
        SA::NUMBER(16,8) as SA
    from trns
)

select * from final