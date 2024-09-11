{{
    config
    (
        post_hook = "
                    INSERT INTO {{ ref('jpndcledw_integration__item_jizen_bunkai_tbl') }} (
                        item_cd,
                        kosei_cd,
                        suryo,
                        koseritsu,
                        MARKER
                        )
                    SELECT c_diprivilegeid,
                        c_diprivilegeid,
                        1,
                        1,
                        2
                    FROM {{ ref('jpndcledw_integration__c_tbecprivilegemst') }};

                    INSERT INTO {{ ref('jpndcledw_integration__item_jizen_bunkai_tbl') }} (
                        item_cd,
                        kosei_cd,
                        suryo,
                        koseritsu,
                        MARKER
                        )
                    SELECT 'X000000001',
                        'X000000001',
                        1,
                        1,
                        3

                    UNION ALL

                    SELECT 'X000000002',
                        'X000000002',
                        1,
                        1,
                        3;
                    "
    )
}}



WITH item_jizen_bunkai_tbl
AS (
    SELECT *
    FROM {{ ref('jpndcledw_integration__item_jizen_bunkai_tbl') }}
    ),
tm14shkos_mainte_work
AS (
    SELECT *
    FROM {{ source('jpdcledw_integration', 'tm14shkos_mainte_work') }}
    ),
t1
AS (
    SELECT tm14.item_cd AS itemcode,
        tm14.kosei_cd AS kosecode,
        tm14.suryo AS suryo,
        tm14.koseritsu AS koseritsu
    FROM item_jizen_bunkai_tbl tm14
    WHERE tm14.item_cd IN (
            SELECT w.itemcode
            FROM tm14shkos_mainte_work w
            )
    ),
final
AS (
    SELECT itemcode::VARCHAR(45) AS itemcode,
        kosecode::VARCHAR(45) AS kosecode,
        suryo::number(13, 4) AS suryo,
        koseritsu::number(16, 8) AS koseritsu,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        NULL::VARCHAR(100) AS inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        NULL::VARCHAR(100) AS updated_by
    FROM t1
    )
SELECT *
FROM final