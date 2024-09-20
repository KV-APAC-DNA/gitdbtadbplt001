with item_zaiko_v as (
    select * from {{ ref('jpndcledw_integration__item_zaiko_v') }}
),

final as (
select  
z_itemcode as "z_itemcode",
z_itemname as "z_itemname",
itemkbn as "itemkbn",
syutoku_kbn as "syutoku_kbn",
bumon7_add_attr1 as "bumon7_add_attr1",
bumon7_add_attr2 as "bumon7_add_attr2",
bumon7_add_attr3 as "bumon7_add_attr3",
bumon7_add_attr4 as "bumon7_add_attr4",
bumon7_add_attr5 as "bumon7_add_attr5",
bumon7_add_attr6 as "bumon7_add_attr6",
bumon7_add_attr7 as "bumon7_add_attr7",
bumon7_add_attr8 as "bumon7_add_attr8",
bumon7_add_attr9 as "bumon7_add_attr9",
bumon7_add_attr10 as "bumon7_add_attr10"
from item_zaiko_v
)

select * from final