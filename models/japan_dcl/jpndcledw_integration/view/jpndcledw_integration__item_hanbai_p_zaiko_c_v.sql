with tm14shkos_qv as (
select * from {{ ref('jpndcledw_integration__tm14shkos_qv') }}
),
syouhincd_henkan_qv as (
select * from {{ ref('jpndcledw_integration__syouhincd_henkan_qv') }}
),
cim08shkos_bunkai_qv as (
select * from {{ ref('jpndcledw_integration__cim08shkos_bunkai_qv') }}
),
final as (
SELECT 
  b.itemcode AS h_o_item_code, 
  c.kosecode AS z_item_code 
FROM 
  (
    (
      syouhincd_henkan_qv a 
      JOIN cim08shkos_bunkai_qv b ON (
        (
          (a.itemcode):: text = (b.bunkai_itemcode):: text
        )
      )
    ) 
    LEFT JOIN tm14shkos_qv c ON (
      (
        (a.koseiocode):: text = (c.itemcode):: text
      )
    )
  )
)
select * from final