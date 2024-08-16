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
        sysdate()::timestamp_ntz(9) AS inserted_date,
        NULL::VARCHAR(100) AS inserted_by,
        sysdate()::timestamp_ntz(9) AS updated_date,
        NULL::VARCHAR(100) AS updated_by
    FROM t1
    )
SELECT *
FROM final