with item_jizen_bunkai_maint as (
select * from {{ ref('jpndcledw_integration__item_jizen_bunkai_maint') }} 
),
final as (
SELECT 
  item_jizen_bunkai_maint.itemcode, 
  item_jizen_bunkai_maint.kosecode, 
  item_jizen_bunkai_maint.suryo, 
  item_jizen_bunkai_maint.koseritsu, 
  item_jizen_bunkai_maint.inserted_date, 
  item_jizen_bunkai_maint.inserted_by, 
  item_jizen_bunkai_maint.updated_date, 
  item_jizen_bunkai_maint.updated_by 
FROM 
  item_jizen_bunkai_maint
)
select * from final