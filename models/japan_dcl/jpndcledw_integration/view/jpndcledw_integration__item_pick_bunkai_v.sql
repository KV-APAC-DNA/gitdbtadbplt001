WITH item_pick_bunkai_tbl
AS (
    SELECT *
    FROM {{'jpndcledw_integration__item_pick_bunkai_tbl'}}
    ),
final
AS (
    SELECT item_pick_bunkai_tbl.itemcode,
        item_pick_bunkai_tbl.itemname,
        item_pick_bunkai_tbl.bunkai_itemcode,
        item_pick_bunkai_tbl.bunkai_itemname,
        (item_pick_bunkai_tbl.bunkai_tanka)::BIGINT AS bunkai_tanka,
        (item_pick_bunkai_tbl.bunkai_kossu)::BIGINT AS bunkai_kossu,
        (item_pick_bunkai_tbl.bunkai_kosritu)::NUMERIC(38, 4) AS bunkai_kosritu
    FROM item_pick_bunkai_tbl
    )
SELECT *
FROM

final
