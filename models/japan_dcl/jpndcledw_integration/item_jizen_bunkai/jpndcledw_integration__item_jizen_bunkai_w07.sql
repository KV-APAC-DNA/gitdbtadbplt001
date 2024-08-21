{{
    config
    (
        pre_hook = "
                    UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
                    SET KOSERITSU = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSERITSU + (
                            SELECT ROUND(S2.SA * (S2.SURYO / S2.KOSECODE_CNT), 8)
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w15') }} S1
                            -- WHERE S1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                            --     AND S1.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSECODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w15') }} S2
                    WHERE S2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                        AND S2.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSECODE;
                    "
    )
}}


with ITEM_JIZEN_BUNKAI_W16 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w16') }}
),

ITEM_JIZEN_BUNKAI_W06 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

trns as
(	
    SELECT T.ITEMCODE AS ITEMCODE,
        S.KOSECODEMAX AS KOSECODE,
        T.KOSERITSUMAX AS KOSERITSU
    FROM (
        SELECT T1.ITEMCODE AS ITEMCODE,
            MAX(T1.KOSERITSU) AS KOSERITSUMAX
        FROM ITEM_JIZEN_BUNKAI_W06 T1
        GROUP BY T1.ITEMCODE
        ) T
    JOIN (
        SELECT S1.ITEMCODE AS ITEMCODE,
            S1.KOSERITSU AS KOSERITSU,
            MAX(S1.KOSECODE) AS KOSECODEMAX
        FROM ITEM_JIZEN_BUNKAI_W06 S1
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