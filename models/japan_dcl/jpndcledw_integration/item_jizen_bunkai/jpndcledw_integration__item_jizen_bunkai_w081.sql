{{
    config
    (
        pre_hook = "
                    UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
                    SET KOSERITSU = (
                            SELECT ROUND(SHKOS.SURYO / T1.SURYO, 8) AS KOSERITSU
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }} SHKOS
                            -- INNER JOIN {{ ref('jpndcledw_integration__item_jizen_bunkai_w012') }} T1 ON SHKOS.ITEMCODE = T1.ITEMCODE
                            -- WHERE SHKOS.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                            --     AND SHKOS.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSECODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }} SHKOS
                        INNER JOIN {{ ref('jpndcledw_integration__item_jizen_bunkai_w012') }} T1 ON SHKOS.ITEMCODE = T1.ITEMCODE
                        WHERE SHKOS.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                            AND SHKOS.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSECODE;
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_WEND as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
),

trns as
(	
    SELECT T.ITEMCODE AS ITEMCODE,
        SUM(T.KOSERITSU) AS KOSERITSUKEI,
        1 - SUM(T.KOSERITSU) AS SA
    FROM ITEM_JIZEN_BUNKAI_WEND T
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