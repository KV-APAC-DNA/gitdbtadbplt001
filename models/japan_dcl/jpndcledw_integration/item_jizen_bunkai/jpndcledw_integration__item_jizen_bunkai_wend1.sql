{{
    config
    (
        pre_hook = "
                    UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
                    SET KOSERITSU = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSERITSU + (
                            SELECT S2.SA
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w083') }} S1
                            -- WHERE S1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w083') }} S2,
                        {{ ref('jpndcledw_integration__item_jizen_bunkai_w071') }} S3
                    WHERE S2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                        AND S3.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.ITEMCODE
                        AND S3.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}.KOSECODE;

                    INSERT INTO {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }} (
                        ITEMCODE,
                        KOSECODE,
                        SURYO,
                        KOSERITSU
                        )
                    SELECT ITEMCODE,
                        KOSECODE,
                        SURYO,
                        0
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w99') }};
                    ",

        post_hook = "
                    INSERT INTO {{this}} (
                        ITEMCODE,
                        KOSECODE,
                        SURYO,
                        KOSERITSU,
                        BUNKAIKBN
                        )
                    SELECT M03.ITEMCODE AS ITEMCODE,
                        M03.ITEMCODE AS KOSECODE,
                        1 AS SURYO,
                        1 AS KOSERITSU,
                        0 AS BUNKAIKBN
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wz') }} M03
                    WHERE NOT EXISTS (
                            SELECT 'X'
                            FROM {{this}} W3
                            WHERE W3.ITEMCODE = M03.ITEMCODE
                            );

                    INSERT INTO {{this}} (
                        ITEMCODE,
                        KOSECODE,
                        SURYO,
                        KOSERITSU,
                        BUNKAIKBN
                        )
                    SELECT M03.ITEMCODE AS ITEMCODE,
                        M03.ITEMCODE AS KOSECODE,
                        1 AS SURYO,
                        1 AS KOSERITSU,
                        0 AS BUNKAIKBN
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_wh') }} M03
                    WHERE NOT EXISTS (
                            SELECT 'X'
                            FROM {{this}} W3
                            WHERE W3.ITEMCODE = M03.ITEMCODE
                            );
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_w071 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w071') }}
),

ITEM_JIZEN_BUNKAI_WEND as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_wend') }}
),

trns as
(	
    SELECT ITEMCODE,
        KOSECODE,
        SURYO,
        KOSERITSU,
        1 AS BUNKAIKBN
    FROM ITEM_JIZEN_BUNKAI_WEND
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSECODE::VARCHAR(20) as KOSECODE,
        SURYO::NUMBER(16,8) as SURYO,
        KOSERITSU::NUMBER(16,8) as KOSERITSU,
        BUNKAIKBN::VARCHAR(10) as BUNKAIKBN
    from trns
)

select * from final