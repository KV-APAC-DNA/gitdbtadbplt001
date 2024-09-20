{{
    config
    (
        pre_hook = "
                    DELETE
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
                    WHERE EXISTS (
                            SELECT 'X'
                            FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w99') }} T2
                            WHERE {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}.KOSECODE = T2.KOSECODE
                            );
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

ITEM_JIZEN_BUNKAI_WZ as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }}
),

trns as
(	
    SELECT T.ITEMCODE AS ITEMCODE,
        SUM(ZAIKO.TANKA * T.SURYO) AS KOTANKASUM
    FROM ITEM_JIZEN_BUNKAI_W3 T
    LEFT OUTER JOIN ITEM_JIZEN_BUNKAI_WZ ZAIKO ON T.KOSECODE = ZAIKO.ITEMCODE
    GROUP BY T.ITEMCODE
),

final as
(
    select
        ITEMCODE::VARCHAR(40) as ITEMCODE,
      	KOTANKASUM::NUMBER(16,8) as KOTANKASUM
    from trns
)

select * from final