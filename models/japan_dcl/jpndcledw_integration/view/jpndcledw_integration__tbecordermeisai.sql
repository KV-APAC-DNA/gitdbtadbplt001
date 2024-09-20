with tbecordermeisai as (
select * from {{ ref('jpndclitg_integration__tbecordermeisai') }}
),
final as (
SELECT 
  tbecordermeisai.dimeisaiid, 
  tbecordermeisai.diorderid, 
  tbecordermeisai.dsitemid, 
  tbecordermeisai.dsitemname, 
  tbecordermeisai.dicancel, 
  tbecordermeisai.dielimflg 
FROM 
  tbecordermeisai
)
select * from final