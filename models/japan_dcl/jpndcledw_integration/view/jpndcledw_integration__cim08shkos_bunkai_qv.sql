with item_pick_bunkai_tbl as (
select * from {{ ref('jpndcledw_integration__item_pick_bunkai_tbl') }}
),
final as (
SELECT 
  item_pick_bunkai_tbl.itemcode, 
  (item_pick_bunkai_tbl.itemname):: character varying(192) AS itemname, 
  (item_pick_bunkai_tbl.itemcname):: character varying(240) AS itemcname, 
  (item_pick_bunkai_tbl.tanka):: character varying(21) AS tanka, 
  (
    item_pick_bunkai_tbl.chubuncode
  ):: character varying(6000) AS chubuncode, 
  item_pick_bunkai_tbl.chubunname, 
  (
    item_pick_bunkai_tbl.chubuncname
  ):: character varying(6103) AS chubuncname, 
  (
    item_pick_bunkai_tbl.daibuncode
  ):: character varying(7) AS daibuncode, 
  item_pick_bunkai_tbl.daibunname, 
  (
    item_pick_bunkai_tbl.daibuncname
  ):: character varying(110) AS daibuncname, 
  (
    item_pick_bunkai_tbl.daidaibuncode
  ):: character varying(6000) AS daidaibuncode, 
  item_pick_bunkai_tbl.daidaibunname, 
  (
    item_pick_bunkai_tbl.daidaibuncname
  ):: character varying(6103) AS daidaibuncname, 
  (
    item_pick_bunkai_tbl.bunkai_itemcode
  ):: character varying(65535) AS bunkai_itemcode, 
  (
    item_pick_bunkai_tbl.bunkai_itemname
  ):: character varying(65535) AS bunkai_itemname, 
  (
    item_pick_bunkai_tbl.bunkai_itemcname
  ):: character varying(65535) AS bunkai_itemcname, 
  item_pick_bunkai_tbl.bunkai_tanka, 
  (
    item_pick_bunkai_tbl.bunkai_kossu
  ):: numeric(36, 18) AS bunkai_kossu, 
  item_pick_bunkai_tbl.bunkai_kosritu, 
  item_pick_bunkai_tbl.insertdate 
FROM 
  item_pick_bunkai_tbl item_pick_bunkai_tbl

)
select * from final
