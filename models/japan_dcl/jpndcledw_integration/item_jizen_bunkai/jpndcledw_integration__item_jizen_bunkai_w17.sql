{{
    config
    (
        pre_hook = "
                   UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
                    SET KOSERITSU = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSERITSU + (
                            SELECT ROUND(S2.SA * ({{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSERITSU / S2.KOSERITSUKEI), 8)
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }} S1
                            -- WHERE S1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                            --     AND S1.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSECODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }} S2
                    WHERE S2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                        AND S2.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSECODE
                        AND SA < 0; 
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_W11 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w11') }}
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