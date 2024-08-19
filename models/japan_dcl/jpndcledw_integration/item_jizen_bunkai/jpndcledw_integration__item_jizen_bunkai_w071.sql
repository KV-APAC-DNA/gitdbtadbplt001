{{
    config
    (
        pre_hook = "
                    UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
                    SET KOSERITSU = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSERITSU + (
                            SELECT ROUND(S2.SA * ({{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSERITSU / S2.KOSERITSUKEI), 8)
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w12') }} S1
                            -- WHERE S1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                            --     AND S1.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSECODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w12') }} S2
                    WHERE S2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                        AND S2.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSECODE;
                    "
    )
}}

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
    SELECT T.ITEMCODE AS ITEMCODE,
        S.KOSECODEMAX AS KOSECODE,
        T.KOSERITSUMAX AS KOSERITSU
    FROM (
        SELECT T1.ITEMCODE AS ITEMCODE,
            MAX(T1.KOSERITSU) AS KOSERITSUMAX
        FROM ITEM_JIZEN_BUNKAI_WEND T1
        GROUP BY T1.ITEMCODE
        ) T
    JOIN (
        SELECT S1.ITEMCODE AS ITEMCODE,
            S1.KOSERITSU AS KOSERITSU,
            MAX(S1.KOSECODE) AS KOSECODEMAX
        FROM ITEM_JIZEN_BUNKAI_WEND S1
        GROUP BY S1.ITEMCODE,
            S1.KOSERITSU
        ) S ON T.ITEMCODE = S.ITEMCODE
        AND T.KOSERITSUMAX = S.KOSERITSU
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