with ITEM_JIZEN_BUNKAI_W06 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

ITEM_JIZEN_BUNKAI_W08 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w08') }}
),

trns as
(	
    SELECT T1.ITEMCODE AS ITEMCODE,
        T1.KOSECODE AS KOSECODE,
        T1.KOSERITSU AS KOSERITSU,
        T2.KOSERITSUKEI AS KOSERITSUKEI,
        T2.SA AS SA
    FROM ITEM_JIZEN_BUNKAI_W06 T1,
        ITEM_JIZEN_BUNKAI_W08 T2
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