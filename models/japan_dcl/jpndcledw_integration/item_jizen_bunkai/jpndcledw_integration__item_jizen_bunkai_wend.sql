{{
    config
    (
        pre_hook = "
                    UPDATE {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
                    SET KOSERITSU = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSERITSU + (
                            SELECT S2.SA
                            -- FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w082') }} S1
                            -- WHERE S1.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                            )
                    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_w082') }} S2,
                        {{ ref('jpndcledw_integration__item_jizen_bunkai_w07') }} S3
                    WHERE S2.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                        AND S3.ITEMCODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.ITEMCODE
                        AND S3.KOSECODE = {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}.KOSECODE;
                    "
    )
}}

with ITEM_JIZEN_BUNKAI_W7 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w7') }}
),

ITEM_JIZEN_BUNKAI_W07 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w07') }}
),

ITEM_JIZEN_BUNKAI_W06 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w06') }}
),

ITEM_JIZEN_BUNKAI_W3 as
(
    select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_w3') }}
),

trns as
(	
    SELECT a.ITEMCODE AS ITEMCODE,
        a.KOSECODE AS KOSECODE,
        a.KOSERITSU AS KOSERITSU,
        b.SURYO AS SURYO
    FROM ITEM_JIZEN_BUNKAI_W06 a,
        ITEM_JIZEN_BUNKAI_W3 b
    WHERE a.ITEMCODE = b.ITEMCODE
        AND a.KOSECODE = b.KOSECODE
),

final as
(
    select
        ITEMCODE::VARCHAR(20) as ITEMCODE,
        KOSECODE::VARCHAR(20) as KOSECODE,
        KOSERITSU::NUMBER(16,8) as KOSERITSU,
        SURYO::NUMBER(16,8) as SURYO
    from trns
)

select * from final