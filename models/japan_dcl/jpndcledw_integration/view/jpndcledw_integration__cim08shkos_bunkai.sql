with item_pick_bunkai_tbl as (
    select * from {{ ref('jpndcledw_integration__item_pick_bunkai_tbl') }}
),

final as (
SELECT item_pick_bunkai_tbl.itemcode,
    item_pick_bunkai_tbl.itemname,
    item_pick_bunkai_tbl.itemcname,
    (item_pick_bunkai_tbl.tanka)::BIGINT AS tanka,
    item_pick_bunkai_tbl.chubuncode,
    item_pick_bunkai_tbl.chubunname,
    item_pick_bunkai_tbl.chubuncname,
    item_pick_bunkai_tbl.daibuncode,
    item_pick_bunkai_tbl.daibunname,
    item_pick_bunkai_tbl.daibuncname,
    item_pick_bunkai_tbl.daidaibuncode,
    item_pick_bunkai_tbl.daidaibunname,
    item_pick_bunkai_tbl.daidaibuncname,
    item_pick_bunkai_tbl.bunkai_itemcode,
    item_pick_bunkai_tbl.bunkai_itemname,
    item_pick_bunkai_tbl.bunkai_itemcname,
    item_pick_bunkai_tbl.bunkai_tanka,
    item_pick_bunkai_tbl.bunkai_kossu,
    item_pick_bunkai_tbl.bunkai_kosritu,
    item_pick_bunkai_tbl.insertdate
FROM item_pick_bunkai_tbl item_pick_bunkai_tbl
WHERE (item_pick_bunkai_tbl.marker = 1)
)

select * from final