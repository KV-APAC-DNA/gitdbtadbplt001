with c_tbtelcompanymst as (
select * from {{ ref('jpndclitg_integration__c_tbtelcompanymst') }}
),
final as (
SELECT 
  c_tbtelcompanymst.c_dstelcompanycd, 
  c_tbtelcompanymst.c_dstelcompayname, 
  c_tbtelcompanymst.didisporder, 
  c_tbtelcompanymst.dielimflg 
FROM 
  c_tbtelcompanymst
  )
select * from final
