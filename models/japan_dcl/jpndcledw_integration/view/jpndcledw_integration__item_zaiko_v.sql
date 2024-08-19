with item_zaiko_tbl as (
select * from {{ ref('jpndcledw_integration__item_zaiko_tbl') }}
),
final as (
SELECT 
  izt.z_itemcode, 
  izt.z_itemname, 
  izt.itemkbn, 
  izt.syutoku_kbn, 
  izt.bumon7_add_attr1, 
  izt.bumon7_add_attr2, 
  izt.bumon7_add_attr3, 
  izt.bumon7_add_attr4, 
  izt.bumon7_add_attr5, 
  izt.bumon7_add_attr6, 
  izt.bumon7_add_attr7, 
  izt.bumon7_add_attr8, 
  izt.bumon7_add_attr9, 
  izt.bumon7_add_attr10 
FROM 
item_zaiko_tbl izt
)
select * from final