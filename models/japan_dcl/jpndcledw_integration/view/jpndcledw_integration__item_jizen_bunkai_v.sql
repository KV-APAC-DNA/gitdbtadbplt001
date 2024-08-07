WITH item_jizen_bunkai_tbl
AS (
    SELECT *
    FROM {{ref('jpndcledw_integration__item_jizen_bunkai_tbl')}}
    ),
final
AS (
    SELECT item_jizen_bunkai_tbl.item_cd,
        item_jizen_bunkai_tbl.kosei_cd,
        item_jizen_bunkai_tbl.suryo,
        item_jizen_bunkai_tbl.koseritsu,
        item_jizen_bunkai_tbl.insertdate,
        item_jizen_bunkai_tbl.inserttime,
        item_jizen_bunkai_tbl.insertid,
        item_jizen_bunkai_tbl.bunkaikbn,
        NULL AS join_rec_upddate,
        item_jizen_bunkai_tbl.inserted_date,
        item_jizen_bunkai_tbl.inserted_by,
        item_jizen_bunkai_tbl.updated_date,
        item_jizen_bunkai_tbl.updated_by
    FROM item_jizen_bunkai_tbl
    WHERE (item_jizen_bunkai_tbl.marker = 1)
    )
SELECT *
FROM final
