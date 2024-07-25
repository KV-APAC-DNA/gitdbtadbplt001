with c_tbtelcompanymst as (
select * from DEV_DNA_CORE.JPDCLITG_INTEGRATION.C_TBTELCOMPANYMST
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
