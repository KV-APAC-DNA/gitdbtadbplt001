with tbecordermeisai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDERMEISAI
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